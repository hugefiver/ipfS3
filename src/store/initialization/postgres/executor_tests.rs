use super::*;
use crate::store::migrations::bridge::Manager;
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement, Value};

async fn pool() -> Option<PgPool> {
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL").ok()?;
    Some(
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("PostgreSQL test fixture must connect"),
    )
}

fn statement(sql: &str) -> Statement {
    Statement::from_string(DatabaseBackend::Postgres, sql)
}

async fn pid(pool: &PgPool) -> i32 {
    sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn postgres_executor_preserves_bindings_rows_probes_and_session_settings() {
    let Some(pool) = pool().await else {
        return;
    };
    let original = pid(&pool).await;
    pool.execute("SET statement_timeout='17s'; SET lock_timeout='13s'")
        .await
        .unwrap();
    let stop = Stop::new(Instant::now() + super::super::BUDGET);
    own_work(pool.clone(), stop, |db| async move {
        let root = db.begin().await?;
        let instant = chrono::DateTime::from_timestamp(1_700_000_000, 123_000_000).unwrap();
        let uuid = uuid::Uuid::new_v4();
        let values: Vec<Value> = vec![
            true.into(), (-7_i16).into(), 42_i32.into(), 5_000_000_000_i64.into(),
            1.25_f32.into(), 2.5_f64.into(), "quote ' and $1".into(), vec![0_u8, 255].into(),
            serde_json::json!({"enabled": true}).into(), instant.into(), uuid.into(),
            Option::<String>::None.into(), Option::<i64>::None.into(),
            instant.date_naive().into(), instant.time().into(), instant.naive_utc().into(),
            Option::<bool>::None.into(), Option::<chrono::DateTime<chrono::Utc>>::None.into(),
        ];
        let row = db.query_one(Statement::from_sql_and_values(DatabaseBackend::Postgres,
            "SELECT $1 AS b, $2 AS small, $3 AS integer, $4 AS big, $5 AS float, $6 AS double, $7 AS text, $8 AS bytes, $9 AS json, $10 AS instant, $11 AS uuid, $12 AS null_text, $13 AS null_big, $14 AS date, $15 AS time, $16 AS naive, $17 AS null_bool, $18 AS null_time, pg_typeof($12)::text AS text_type, pg_typeof($13)::text AS big_type, pg_typeof($17)::text AS bool_type, pg_typeof($18)::text AS time_type", values)).await?.unwrap();
        assert!(row.try_get::<bool>("", "b")?);
        assert_eq!(row.try_get::<i16>("", "small")?, -7);
        assert_eq!(row.try_get::<i32>("", "integer")?, 42);
        assert_eq!(row.try_get::<i64>("", "big")?, 5_000_000_000);
        assert_eq!(row.try_get::<f32>("", "float")?, 1.25);
        assert_eq!(row.try_get::<f64>("", "double")?, 2.5);
        assert_eq!(row.try_get::<String>("", "text")?, "quote ' and $1");
        assert_eq!(row.try_get::<Vec<u8>>("", "bytes")?, [0, 255]);
        assert_eq!(row.try_get::<serde_json::Value>("", "json")?, serde_json::json!({"enabled": true}));
        assert_eq!(row.try_get::<chrono::DateTime<chrono::Utc>>("", "instant")?, instant);
        assert_eq!(row.try_get::<uuid::Uuid>("", "uuid")?, uuid);
        assert_eq!(row.try_get::<Option<String>>("", "null_text")?, None);
        assert_eq!(row.try_get::<Option<i64>>("", "null_big")?, None);
        assert_eq!(row.try_get::<Option<bool>>("", "null_bool")?, None);
        assert_eq!(row.try_get::<Option<chrono::DateTime<chrono::Utc>>>("", "null_time")?, None);
        for (column, expected) in [("text_type", "text"), ("big_type", "bigint"), ("bool_type", "boolean"), ("time_type", "timestamp with time zone")] {
            assert_eq!(row.try_get::<String>("", column)?, expected);
        }
        assert_eq!(row.try_get::<chrono::NaiveDate>("", "date")?, instant.date_naive());
        assert_eq!(row.try_get::<chrono::NaiveTime>("", "time")?, instant.time());
        assert_eq!(row.try_get::<chrono::NaiveDateTime>("", "naive")?, instant.naive_utc());

        let schema = format!("migration_probe_{}", uuid::Uuid::new_v4().simple());
        let other = format!("migration_probe_{}", uuid::Uuid::new_v4().simple());
        db.execute_unprepared(&format!("CREATE SCHEMA {schema}; CREATE SCHEMA {other}; SET LOCAL search_path TO {schema}; CREATE TABLE rows (n INTEGER); CREATE VIEW visible_view AS SELECT n FROM rows; CREATE TABLE {other}.hidden (n INTEGER)")).await?;
        let manager = Manager::guarded(db.clone());
        assert!(manager.has_table("rows").await?);
        assert!(!manager.has_table("visible_view").await?);
        assert!(!manager.has_table("hidden").await?);
        assert!(manager.has_column("rows", "n").await?);
        assert!(!manager.has_column("rows", "missing").await?);
        assert!(!manager.has_column("hidden", "n").await?);
        assert_eq!(db.execute(statement("INSERT INTO rows VALUES (1),(2),(3)")).await?.rows_affected(), 3);
        let nested = db.begin().await?;
        assert_eq!(db.execute(statement("UPDATE rows SET n=n+10 WHERE n>1")).await?.rows_affected(), 2);
        nested.rollback().await?;
        let nested = db.begin().await?;
        db.execute_unprepared("INSERT INTO rows VALUES (4)").await?;
        nested.commit().await?;
        let started = Instant::now();
        let first = db.query_one(statement("SELECT n, pg_sleep(0.04) FROM rows ORDER BY n")).await?.unwrap();
        assert_eq!(first.try_get::<i32>("", "n")?, 1);
        assert!(started.elapsed() >= Duration::from_millis(140));
        assert_eq!(db.query_all(statement("SELECT n FROM rows ORDER BY n")).await?.len(), 4);
        root.rollback().await
    }).await.unwrap();
    assert_eq!(pid(&pool).await, original);
    let settings: (String, String) = sqlx::query_as(
        "SELECT current_setting('statement_timeout'), current_setting('lock_timeout')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(settings, ("17s".into(), "13s".into()));
    pool.close().await;
}

#[tokio::test]
async fn postgres_executor_command_gap_cancellation_refuses_nested_marker_and_commit() {
    let Some(pool) = pool().await else {
        return;
    };
    let original = pid(&pool).await;
    let stop = Stop::new(Instant::now() + super::super::BUDGET);
    let cancel = stop.clone();
    let result = own_work(pool.clone(), stop, |db| async move {
        let root = db.begin().await?;
        db.execute_unprepared("CREATE TEMP TABLE startup_gap (n INTEGER)")
            .await?;
        db.execute_unprepared("INSERT INTO startup_gap VALUES (1)")
            .await?;
        cancel.cancel(1);
        assert!(db.begin().await.is_err());
        assert!(
            db.execute_unprepared("INSERT INTO startup_gap VALUES (2)")
                .await
                .is_err()
        );
        assert!(root.commit().await.is_err());
        Ok(())
    })
    .await;
    assert!(result.is_err());
    assert_eq!(pid(&pool).await, original);
    let absent: bool = sqlx::query_scalar("SELECT to_regclass('pg_temp.startup_gap') IS NULL")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(absent);
    pool.close().await;
}

#[tokio::test]
async fn postgres_executor_deadline_is_shared_across_successive_commands() {
    let Some(pool) = pool().await else {
        return;
    };
    let original = pid(&pool).await;
    let started = Instant::now();
    let stop = Stop::new(started + Duration::from_secs(1));
    let observation = stop.clone();
    let result: Result<(), DbErr> = own_work(pool.clone(), stop, |db| async move {
        let _root = db.begin().await?;
        db.query_one(statement("SELECT pg_sleep(0.6)")).await?;
        db.query_one(statement("SELECT pg_sleep(1)")).await?;
        panic!("the second statement exceeded the shared deadline");
    })
    .await;
    assert!(result.is_err());
    assert_eq!(
        observation
            .0
            .reason
            .load(std::sync::atomic::Ordering::Acquire),
        2
    );
    assert!(started.elapsed() < Duration::from_millis(1400));
    assert_eq!(pid(&pool).await, original);
    pool.close().await;
}

#[test]
fn postgres_bindings_reject_unsigned_overflow() {
    let statement =
        Statement::from_sql_and_values(DatabaseBackend::Postgres, "SELECT $1", [u64::MAX.into()]);
    assert!(binding::arguments(&statement).is_err());
}

#[tokio::test]
async fn postgres_executor_unwind_disconnects_the_unclean_backend() {
    let Some(pool) = pool().await else {
        return;
    };
    let mut observer = PgConnection::connect_with(&pool.connect_options())
        .await
        .unwrap_or_else(|_| panic!("could not connect unwind observer"));
    let original: (i32, chrono::DateTime<chrono::Utc>) = sqlx::query_as(
        "SELECT pid, backend_start FROM pg_catalog.pg_stat_activity WHERE pid=pg_backend_pid()",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or_else(|_| panic!("could not identify unwind backend"));
    let unique = uuid::Uuid::new_v4();
    let schema = format!("migration_unwind_{}", unique.simple());
    let marker = format!("startup_unwind_{}", unique.simple());
    let key = unique.as_u128() as i64;
    observer
        .execute(format!("CREATE SCHEMA {schema}").as_str())
        .await
        .unwrap_or_else(|_| panic!("could not create unwind schema"));

    let work_schema = schema.clone();
    let work_marker = marker.clone();
    let work_pool = pool.clone();
    let mut owner = tokio::spawn(async move {
        own_work::<(), _, _>(
            work_pool,
            Stop::new(Instant::now() + super::super::BUDGET),
            |db| async move {
                db.execute(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    "SELECT set_config('application_name', $1, false)",
                    [work_marker.into()],
                ))
                .await?;
                let _root = db.begin().await?;
                db.execute_unprepared(&format!(
                    "CREATE TABLE {work_schema}.uncommitted (n INTEGER)"
                ))
                .await?;
                db.query_one(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    "SELECT pg_advisory_xact_lock($1)",
                    [key.into()],
                ))
                .await?;
                panic!("intentional PostgreSQL startup owner unwind");
            },
        )
        .await
    });
    let mut failures = Vec::new();
    match tokio::time::timeout(Duration::from_secs(5), &mut owner).await {
        Ok(Err(error)) if error.is_panic() => {}
        Ok(_) => failures.push("startup owner did not reach the intentional panic"),
        Err(_) => {
            owner.abort();
            let _ = owner.await;
            failures.push("startup owner did not finish");
        }
    }
    let terminated = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let gone: bool = sqlx::query_scalar(
                "SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid=$1 AND backend_start=$2)",
            )
            .bind(original.0)
            .bind(original.1)
            .fetch_one(&mut observer)
            .await
            .map_err(|_| ())?;
            if gone {
                return Ok::<_, ()>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    if !matches!(terminated, Ok(Ok(()))) {
        failures.push("unclean original backend did not terminate");
    }
    let unlocked = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock($1)")
        .bind(key)
        .fetch_one(&mut observer)
        .await;
    if !matches!(unlocked, Ok(true)) {
        failures.push("unwound backend retained its advisory lock");
    }
    let absent = sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NULL")
        .bind(format!("{schema}.uncommitted"))
        .fetch_one(&mut observer)
        .await;
    if !matches!(absent, Ok(true)) {
        failures.push("unwound startup published transactional DDL");
    }
    let borrower = tokio::time::timeout(
        Duration::from_secs(5),
        sqlx::query_scalar::<_, String>("SELECT current_setting('application_name')")
            .fetch_one(&pool),
    )
    .await;
    if !matches!(borrower, Ok(Ok(value)) if value != marker) {
        failures.push("subsequent pool borrower retained the session marker or could not run");
    }
    let subsequent = tokio::time::timeout(
        Duration::from_secs(5),
        own_work(
            pool.clone(),
            Stop::new(Instant::now() + super::super::BUDGET),
            |db| async move {
                let root = db.begin().await?;
                db.query_one(statement("SELECT 1")).await?;
                root.rollback().await
            },
        ),
    )
    .await;
    if !matches!(subsequent, Ok(Ok(()))) {
        failures.push("startup executor did not recover after unwind");
    }

    // Close any contaminated pool connection before dropping the private schema.
    if tokio::time::timeout(Duration::from_secs(5), pool.close())
        .await
        .is_err()
    {
        failures.push("unwind pool cleanup did not finish");
    }
    if !matches!(
        tokio::time::timeout(
            Duration::from_secs(5),
            observer.execute(format!("DROP SCHEMA {schema} CASCADE").as_str()),
        )
        .await,
        Ok(Ok(_))
    ) {
        failures.push("unwind schema cleanup failed");
    }
    if close_until(Instant::now() + Duration::from_secs(5), observer.close())
        .await
        .is_err()
    {
        failures.push("unwind observer cleanup failed");
    }
    assert!(failures.is_empty(), "{failures:?}");
}

#[tokio::test(start_paused = true)]
async fn startup_shutdown_shares_the_remaining_budget_and_drops_owned_futures() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct Probe(Arc<AtomicUsize>);
    impl Drop for Probe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    async fn stalled(probe: Probe) -> Result<(), sqlx::Error> {
        let _owned = probe;
        std::future::pending().await
    }

    let stop = Stop::new(Instant::now() + super::super::BUDGET);
    stop.cancel(1);
    let deadline = stop.cleanup_deadline();
    tokio::time::advance(Duration::from_secs(3)).await;
    let drops = Arc::new(AtomicUsize::new(0));
    assert!(
        close_until(deadline, stalled(Probe(drops.clone())))
            .await
            .is_err()
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(Instant::now() >= deadline);
    assert!(Instant::now() < deadline + Duration::from_millis(10));
    let exhausted = Instant::now();
    assert!(
        close_until(deadline, stalled(Probe(drops.clone())))
            .await
            .is_err()
    );
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert!(Instant::now() - exhausted < Duration::from_millis(10));
    assert_eq!(stop.cleanup_deadline(), deadline);
}

#[tokio::test(start_paused = true)]
async fn startup_stop_keeps_the_first_reason_and_absolute_deadline() {
    let deadline = Instant::now() + Duration::from_secs(60);
    let stop = Stop::new(deadline);
    tokio::time::advance(Duration::from_secs(59)).await;
    assert!(stop.check().is_ok());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(stop.check().is_err());
    stop.cancel(1);
    assert_eq!(stop.0.reason.load(std::sync::atomic::Ordering::Acquire), 2);
    assert_eq!(stop.0.deadline, deadline);
}
