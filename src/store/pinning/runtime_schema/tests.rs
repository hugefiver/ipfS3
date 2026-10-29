use super::*;
use crate::store::pinning::management::native;
use crate::store::{self, pinning::management::records::SharedDecodeBudget};
use sea_orm::{ConnectOptions, Database};

mod shadows;

async fn custom_collation(
    name: &'static str,
    compare: fn(&str, &str) -> std::cmp::Ordering,
) -> Store {
    let mut options = ConnectOptions::new("sqlite::memory:");
    options
        .max_connections(1)
        .map_sqlx_sqlite_opts(move |options| options.collation(name, compare));
    let db = Database::connect(options).await.unwrap();
    store::run_migrations(&db).await.unwrap();
    Store::new(db)
}

async fn fixture() -> Store {
    let mut options = ConnectOptions::new("sqlite::memory:");
    options.max_connections(1);
    let db = Database::connect(options).await.unwrap();
    store::run_migrations(&db).await.unwrap();
    Store::new(db)
}

async fn qualify(store: &Store) -> AppResult<()> {
    let work = SharedDecodeBudget::default();
    store
        .qualify_runtime_schema(DecodeWork::shared(&work))
        .await
}

async fn guard(db: &impl ConnectionTrait) -> String {
    db.query_one(Statement::from_string(
        DatabaseBackend::Sqlite,
        "SELECT sql FROM main.sqlite_schema WHERE name='s75_applied_no_delete'",
    ))
    .await
    .unwrap()
    .unwrap()
    .try_get_by_index(0)
    .unwrap()
}

#[tokio::test]
async fn clones_reuse_qualification_but_new_stores_validate_their_own_database() {
    let store = fixture().await;
    qualify(&store).await.unwrap();
    let clone = store.clone();
    let warm_work = SharedDecodeBudget::default();
    clone
        .qualify_runtime_schema(DecodeWork::shared(&warm_work))
        .await
        .unwrap();
    assert_eq!(warm_work.lock().unwrap().storage_usage().sql, 0);
    store
        .db()
        .execute_unprepared("DROP TRIGGER s75_applied_no_delete")
        .await
        .unwrap();
    assert!(qualify(&Store::new(store.db().clone())).await.is_err());
    assert!(management::schema::validate(store.db()).await.is_err());

    let other = fixture().await;
    other
        .db()
        .execute_unprepared("DROP TRIGGER s75_applied_no_delete")
        .await
        .unwrap();
    assert!(qualify(&other).await.is_err());
}

#[tokio::test]
async fn startup_qualification_is_reused_by_the_first_runtime_transaction() {
    let mut db = store::connect_database("sqlite::memory:").await.unwrap();
    let trace = crate::sql_trace::SqlTrace::default();
    trace.attach(&mut db);
    let store = Store::open(db).await.unwrap();
    trace.take("SQLite startup", false);
    let work = SharedDecodeBudget::default();
    let tx = store
        .begin_runtime(DecodeWork::shared(&work))
        .await
        .unwrap();
    tx.query_one(Statement::from_string(DatabaseBackend::Sqlite, "SELECT 1"))
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    trace.take("SQLite first transaction after startup", true);
}

#[tokio::test]
#[ignore = "requires an authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_lifecycle_qualification_and_cold_rejection() {
    use futures_util::FutureExt;
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL").expect("explicit test PostgreSQL URL");
    let admin = Database::connect(&url).await.unwrap();
    let schema = format!("qualification_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .unwrap();
    let mut options = ConnectOptions::new(url);
    options
        .max_connections(1)
        .set_schema_search_path(schema.clone());
    let mut db = Database::connect(options).await.unwrap();
    let trace = crate::sql_trace::SqlTrace::default();
    trace.attach(&mut db);
    let outcome = std::panic::AssertUnwindSafe(async {
        let opened = Store::open(db.clone()).await.unwrap();
        // Guarded startup uses its own SQLx executor, outside SeaORM metrics.
        // Observe lazy cold validation below, not a fictitious startup count.
        assert!(opened.inner.qualification.get().is_some());
        let work = SharedDecodeBudget::default();
        let first = opened.clone().begin_runtime(DecodeWork::shared(&work)).await.unwrap();
        first.query_one(Statement::from_string(DatabaseBackend::Postgres, "SELECT 1")).await.unwrap();
        first.rollback().await.unwrap();
        trace.take("PG startup reuse", true);

        let cold = Store::new(db.clone());
        assert_cancelled_checkout_releases_without_qualification(&cold).await;
        let clone = cold.clone();
        let (left, right) = tokio::join!(qualify(&cold), qualify(&clone));
        left.unwrap(); right.unwrap();
        let concurrent = trace.take("PG concurrent cold qualification", false);
        qualify(&Store::new(db.clone())).await.unwrap();
        let single = trace.take("PG independent Store qualification", false);
        assert_eq!(concurrent.1, single.1, "concurrent callers share one validation");

        for (damage, repair) in [
            ("INSERT INTO seaql_migrations(version,applied_at) VALUES('unknown',0)", Some("DELETE FROM seaql_migrations WHERE version='unknown'")),
            ("UPDATE seaql_migrations SET version='missing' WHERE version=(SELECT min(version) FROM seaql_migrations)", None),
            ("UPDATE pin_management_readiness SET state='backfill' WHERE id=1", Some("UPDATE pin_management_readiness SET state='ready' WHERE id=1")),
        ] {
            // Preserve the original migration name rather than relying on a test-only list.
            let first: String = db.query_one(Statement::from_string(DatabaseBackend::Postgres, "SELECT min(version) AS version FROM seaql_migrations")).await.unwrap().unwrap().try_get("", "version").unwrap();
            db.execute_unprepared(damage).await.unwrap();
            let broken = Store::new(db.clone());
            assert!(qualify(&broken).await.is_err());
            assert!(broken.inner.qualification.get().is_none());
            if let Some(repair) = repair { db.execute_unprepared(repair).await.unwrap(); }
            else {
                db.execute(Statement::from_sql_and_values(DatabaseBackend::Postgres, "UPDATE seaql_migrations SET version=$1 WHERE version='missing'", [first.into()])).await.unwrap();
            }
            qualify(&broken).await.unwrap();
        }
        db.execute_unprepared("DROP INDEX uq_pin_leases_owner_object_source").await.unwrap();
        assert!(qualify(&Store::new(db.clone())).await.is_err());
    }).catch_unwind().await;
    db.close().await.unwrap();
    admin
        .execute_unprepared(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .await
        .unwrap();
    admin.close().await.unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn correct_versions_do_not_qualify_damaged_ddl_and_repair_can_retry() {
    let store = fixture().await;
    let ddl = guard(store.db()).await;
    store
        .db()
        .execute_unprepared("DROP TRIGGER s75_applied_no_delete")
        .await
        .unwrap();
    let failed_work = SharedDecodeBudget::default();
    assert!(
        store
            .qualify_runtime_schema(DecodeWork::shared(&failed_work))
            .await
            .is_err()
    );
    assert!(failed_work.lock().unwrap().storage_usage().sql > 0);
    store.db().execute_unprepared(&ddl).await.unwrap();
    qualify(&store).await.unwrap();
}

#[tokio::test]
async fn native_constraints_are_part_of_the_initial_qualification() {
    let store = fixture().await;
    store
        .db()
        .execute_unprepared("DROP INDEX uq_pin_leases_owner_object_source")
        .await
        .unwrap();
    assert!(qualify(&store).await.is_err());
}

#[tokio::test]
async fn one_connection_begin_runtime_leaves_the_first_business_statement_to_the_caller() {
    let store = fixture().await;
    let work = SharedDecodeBudget::default();
    let tx = store
        .begin_runtime(DecodeWork::shared(&work))
        .await
        .unwrap();
    tx.db()
        .execute_unprepared("UPDATE pin_provider_routes SET provider=provider WHERE 1=0")
        .await
        .unwrap();
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn cold_store_rejects_incomplete_versions_and_readiness() {
    for sql in [
        "DELETE FROM seaql_migrations WHERE version=(SELECT max(version) FROM seaql_migrations)",
        "INSERT INTO seaql_migrations(version,applied_at) VALUES('unknown',0)",
        "UPDATE pin_management_readiness SET state='backfill' WHERE id=1",
    ] {
        let store = fixture().await;
        store.db().execute_unprepared(sql).await.unwrap();
        assert!(qualify(&store).await.is_err());
        assert!(store.inner.qualification.get().is_none());
    }
}

#[tokio::test]
async fn warm_versions_use_qualification_without_storage_work() {
    let store = fixture().await;
    qualify(&store).await.unwrap();
    let work = SharedDecodeBudget::default();
    let tx = store
        .begin_runtime(DecodeWork::shared(&work))
        .await
        .unwrap();
    let before = work.lock().unwrap().storage_usage();
    let versions = tx.schema_versions(DecodeWork::shared(&work)).await.unwrap();
    assert_eq!(
        versions,
        management::schema::read_versions(tx.db()).await.unwrap()
    );
    let after = work.lock().unwrap().storage_usage();
    assert_eq!(after.sql - before.sql, 0);
    assert_eq!(after.rows - before.rows, 0);
    assert_eq!(after.cells - before.cells, 0);
    assert_eq!(after.raw_bytes - before.raw_bytes, 0);
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn failed_budget_is_charged_without_publishing_and_releases_the_connection() {
    let store = fixture().await;
    let work = std::sync::Arc::new(std::sync::Mutex::new(
        management::records::ReadSession::for_task(
            native::meter::Budget::new(native::meter::Limits {
                rows: 1,
                ..Default::default()
            }),
            u64::MAX,
            u64::MAX,
            management::transaction::CancellationToken::new(),
        ),
    ));
    assert!(matches!(
        store
            .qualify_runtime_schema(DecodeWork::shared(&work))
            .await,
        Err(AppError::ManagementBudget)
    ));
    let usage = work.lock().unwrap().storage_usage();
    assert!(usage.sql > 0 && usage.rows > 1 && usage.cells > 0 && usage.raw_bytes > 0);
    let writer = store.db().begin().await.unwrap();
    writer
        .execute_unprepared("UPDATE pin_management_readiness SET state='ready' WHERE id=1")
        .await
        .unwrap();
    writer.rollback().await.unwrap();

    let next = SharedDecodeBudget::default();
    store
        .qualify_runtime_schema(DecodeWork::shared(&next))
        .await
        .unwrap();
    assert!(next.lock().unwrap().storage_usage().sql > 0);
}

#[tokio::test]
async fn cancelled_initializer_waiting_for_checkout_rolls_back_without_publication() {
    let store = fixture().await;
    assert_cancelled_checkout_releases_without_qualification(&store).await;
    let next = SharedDecodeBudget::default();
    let tx = store
        .begin_runtime(DecodeWork::shared(&next))
        .await
        .unwrap();
    tx.db()
        .execute_unprepared("UPDATE pin_management_readiness SET state='ready' WHERE id=1")
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(next.lock().unwrap().storage_usage().rows > 0);
}

async fn assert_cancelled_checkout_releases_without_qualification(store: &Store) {
    let checkout = store.db().begin().await.unwrap();
    let budget = native::meter::Budget::default();
    let cancel = budget.cancellation();
    let work = std::sync::Arc::new(std::sync::Mutex::new(
        management::records::ReadSession::for_task(
            budget,
            u64::MAX,
            u64::MAX,
            management::transaction::CancellationToken::new(),
        ),
    ));
    let waiting_store = store.clone();
    let waiting_work = work.clone();
    let mut task = tokio::spawn(async move {
        waiting_store
            .qualify_runtime_schema(DecodeWork::shared(&waiting_work))
            .await
    });
    tokio::select! {
        result = &mut task => panic!("initializer must wait for checkout: {result:?}"),
        _ = async {
            while work.lock().unwrap().storage_usage().sql == 0 {
                tokio::task::yield_now().await;
            }
        } => {}
    }
    cancel.store(true, std::sync::atomic::Ordering::Relaxed);
    checkout.rollback().await.unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(AppError::ManagementBudget)
    ));
    assert!(store.inner.qualification.get().is_none());
}

#[tokio::test]
async fn schema_reads_charge_actual_cells_bytes_and_failed_sql() {
    let store = fixture().await;
    let tx = store.db().begin().await.unwrap();
    let work = SharedDecodeBudget::default();
    let db = meter::SqliteRead::new(&tx, DecodeWork::shared(&work));
    db.query_one(Statement::from_string(
        DatabaseBackend::Sqlite,
        "SELECT 7 AS n,'abc' AS text,x'0102' AS bytes,NULL AS absent,0.5 AS fraction",
    ))
    .await
    .unwrap();
    let used = work.lock().unwrap().storage_usage();
    assert_eq!(used.sql, 1);
    assert_eq!(used.rows, 1);
    assert_eq!(used.cells, 5);
    assert_eq!(used.raw_bytes, 8 + 3 + 2 + 8);
    assert!(
        db.query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT missing_column FROM main.seaql_migrations",
        ))
        .await
        .is_err()
    );
    let used = work.lock().unwrap().storage_usage();
    assert_eq!(used.sql, 2);
    assert_eq!(used.rows, 1);
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn a_waiter_checks_its_own_cancelled_budget_without_inheriting_query_debt() {
    let store = fixture().await;
    let checkout = store.db().begin().await.unwrap();
    let initializer_work = SharedDecodeBudget::default();
    let first_store = store.clone();
    let first_work = initializer_work.clone();
    let mut initializer = tokio::spawn(async move {
        first_store
            .qualify_runtime_schema(DecodeWork::shared(&first_work))
            .await
    });
    tokio::select! {
        result = &mut initializer => panic!("initializer must wait for checkout: {result:?}"),
        _ = async {
            while initializer_work.lock().unwrap().storage_usage().sql == 0 {
                tokio::task::yield_now().await;
            }
        } => {}
    }

    let budget = native::meter::Budget::default();
    let cancel = budget.cancellation();
    let waiter_work = std::sync::Arc::new(std::sync::Mutex::new(
        management::records::ReadSession::for_task(
            budget,
            u64::MAX,
            u64::MAX,
            management::transaction::CancellationToken::new(),
        ),
    ));
    let waiter = store.qualify_runtime_schema(DecodeWork::shared(&waiter_work));
    tokio::pin!(waiter);
    assert!(futures_util::poll!(&mut waiter).is_pending());
    cancel.store(true, std::sync::atomic::Ordering::Relaxed);
    checkout.rollback().await.unwrap();
    initializer.await.unwrap().unwrap();
    assert!(matches!(waiter.await, Err(AppError::ManagementBudget)));
    assert_eq!(waiter_work.lock().unwrap().storage_usage().sql, 0);
    assert!(initializer_work.lock().unwrap().storage_usage().rows > 0);
}
