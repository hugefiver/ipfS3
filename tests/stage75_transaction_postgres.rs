//! Uses only a parent-owned, empty, isolated PostgreSQL schema. No schema DDL,
//! migration, production access, or cleanup of pre-existing state.
#[path = "../src/store/pinning/management/transaction/tests.rs"]
mod sqlite;
use ipfs_s3_gateway::store::pinning::management::{
    ManagementError,
    transaction::{CancellationToken, ManagementTransaction, StopCause},
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, DbErr,
    RuntimeErr, Statement, TransactionTrait,
};
use std::time::{Duration, Instant};

async fn released(db: &DatabaseConnection, id: i64) {
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT count(*) AS n FROM stage75_transaction_marker WHERE id=1",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), 0);
    tokio::time::timeout(Duration::from_secs(2), db.execute_unprepared(
        "INSERT INTO stage75_transaction_marker VALUES(1); DELETE FROM stage75_transaction_marker WHERE id=1",
    )).await.expect("rollback must have released the exact uncommitted key lock").unwrap();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "INSERT INTO stage75_transaction_marker(id) VALUES($1)",
        [id.into()],
    ))
    .await
    .unwrap();
}

async fn wait_for(db: &DatabaseConnection, pid: i32, event: &str) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let row = db.query_one(Statement::from_sql_and_values(DatabaseBackend::Postgres,
                "SELECT wait_event_type='Lock' AS locked, wait_event='PgSleep' AS sleeping FROM pg_stat_activity WHERE pid=$1",
                [pid.into()])).await.unwrap().unwrap();
            if row.try_get::<Option<bool>>("", event).unwrap() == Some(true) { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("observe the actual SQL running/blocked before cancellation");
}

#[tokio::test]
#[ignore = "requires parent-owned isolated IPFS_S3_STAGE75_TRANSACTION_DATABASE_URL"]
async fn postgres_engine_deadline_cancel_and_lock_wait_release_the_actual_transaction() {
    let url = std::env::var("IPFS_S3_STAGE75_TRANSACTION_DATABASE_URL")
        .expect("parent supplies isolated scope");
    let mut options = ConnectOptions::new(url.clone());
    options.max_connections(1);
    let db = Database::connect(options).await.unwrap();
    let observer = Database::connect(url).await.unwrap();
    let row = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT current_schema() AS scope, to_regclass(current_schema() || '.stage75_transaction_marker')::text AS existing"))
        .await.unwrap().unwrap();
    let scope: String = row.try_get("", "scope").unwrap();
    assert!(
        scope.starts_with("stage75_") || scope.starts_with("stage7_"),
        "refuse a shared schema"
    );
    assert!(
        row.try_get::<Option<String>>("", "existing")
            .unwrap()
            .is_none(),
        "preserve existing fixture; require a fresh schema"
    );
    db.execute_unprepared("CREATE TABLE stage75_transaction_marker(id bigint PRIMARY KEY); CREATE TABLE stage75_transaction_lock(id bigint PRIMARY KEY, value bigint NOT NULL); INSERT INTO stage75_transaction_lock VALUES(0,0)").await.unwrap();

    for (index, locked, external_cancel, server_timeout) in [
        (2_i64, false, false, false),
        (3, false, true, false),
        (4, true, false, false),
        (5, true, true, false),
        (6, false, false, true),
        (7, true, false, true),
    ] {
        let blocker = observer.begin().await.unwrap();
        if locked {
            blocker
                .execute_unprepared("UPDATE stage75_transaction_lock SET value=value+1 WHERE id=0")
                .await
                .unwrap();
        }
        let cancel = CancellationToken::new();
        let limit = if external_cancel || server_timeout {
            Duration::from_secs(10)
        } else {
            Duration::from_millis(600)
        };
        let mut tx = ManagementTransaction::begin(&db, limit, cancel.clone())
            .await
            .unwrap();
        let pid = tx
            .run(|tx, _| {
                Box::pin(async move {
                    let row = tx
                        .query_one(Statement::from_string(
                            DatabaseBackend::Postgres,
                            "SELECT pid, backend_start::text AS birth, current_schema() AS scope, current_setting('statement_timeout')::interval BETWEEN interval '1 millisecond' AND interval '31 seconds' AS statement_bounded, current_setting('lock_timeout')::interval BETWEEN interval '1 millisecond' AND interval '31 seconds' AS lock_bounded FROM pg_stat_activity WHERE pid=pg_backend_pid()",
                        ))
                        .await
                        .map_err(|_| ManagementError::Database)?
                        .ok_or(ManagementError::Database)?;
                    assert!(row.try_get::<bool>("", "statement_bounded").unwrap());
                    assert!(row.try_get::<bool>("", "lock_bounded").unwrap());
                    Ok((
                        row.try_get::<i32>("", "pid").unwrap(),
                        row.try_get::<String>("", "scope").unwrap(),
                        row.try_get::<String>("", "birth").unwrap(),
                    ))
                })
            })
            .await
            .unwrap();
        assert_eq!(pid.1, scope, "cloned options must retain search_path");
        let birth = db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "SELECT backend_start::text AS birth FROM pg_stat_activity WHERE pid=$1",
                [pid.0.into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<String>("", "birth")
            .unwrap();
        assert_eq!(
            pid.2, birth,
            "cancellation identity is the actual owned backend incarnation"
        );
        let started = Instant::now();
        let (entered, running) = tokio::sync::oneshot::channel();
        let (foreign_started, foreign_running) = tokio::sync::oneshot::channel();
        let operation = tx.run(move |tx, _| {
            Box::pin(async move {
                tx.execute_unprepared("INSERT INTO stage75_transaction_marker VALUES(1)")
                    .await
                    .map_err(|_| ManagementError::Database)?;
                entered.send(()).unwrap();
                if server_timeout {
                    let setting = if locked {
                        "SET LOCAL lock_timeout='90ms'"
                    } else {
                        "SET LOCAL statement_timeout='90ms'"
                    };
                    tx.execute_unprepared(setting)
                        .await
                        .map_err(|_| ManagementError::Database)?;
                }
                let sql = if locked {
                    "UPDATE stage75_transaction_lock SET value=value+1 WHERE id=0"
                } else {
                    "SELECT pg_sleep(60)"
                };
                let error = tx.execute_unprepared(sql).await.expect_err(
                    "actual SQL must be interrupted, not merely its Rust future abandoned",
                );
                if server_timeout {
                    let DbErr::Exec(RuntimeErr::SqlxError(error)) = &error else {
                        panic!("expected SQLx server error: {error:?}");
                    };
                    let code = error.as_database_error().unwrap().code().unwrap();
                    assert_eq!(code.as_ref(), if locked { "55P03" } else { "57014" });
                }
                Err::<(), _>(ManagementError::Database)
            })
        });
        let observe = async {
            running.await.unwrap();
            if !server_timeout {
                wait_for(&db, pid.0, if locked { "locked" } else { "sleeping" }).await;
            }
            if external_cancel {
                let foreign_pid = foreign_running.await.unwrap();
                assert_ne!(foreign_pid, pid.0);
                wait_for(&db, foreign_pid, "sleeping").await;
                cancel.cancel();
            }
        };
        let unrelated = async {
            if external_cancel {
                let foreign = observer.begin().await.unwrap();
                let row = foreign
                    .query_one(Statement::from_string(
                        DatabaseBackend::Postgres,
                        "SELECT pg_backend_pid() AS pid",
                    ))
                    .await
                    .unwrap()
                    .unwrap();
                foreign_started
                    .send(row.try_get::<i32>("", "pid").unwrap())
                    .unwrap();
                foreign
                    .execute_unprepared("SELECT pg_sleep(1)")
                    .await
                    .expect("cancellation must not interrupt a different backend");
                foreign.rollback().await.unwrap();
            }
        };
        let (result, (), ()) = tokio::join!(operation, observe, unrelated);
        let failure = result.unwrap_err();
        assert_eq!(
            failure.cause,
            if external_cancel {
                StopCause::Cancelled
            } else if server_timeout {
                StopCause::Management(ManagementError::Database)
            } else {
                StopCause::Deadline
            }
        );
        assert!(
            failure.rollback_confirmed,
            "only acknowledged rollback proves stopped"
        );
        assert!(started.elapsed() < Duration::from_secs(3));
        blocker.rollback().await.unwrap();
        released(&db, index).await;
    }
    observer.close().await.unwrap();
    db.close().await.unwrap();
}
