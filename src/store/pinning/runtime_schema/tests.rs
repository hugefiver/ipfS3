use super::*;
use crate::store::{self, pinning::management::records::SharedDecodeBudget};
use sea_orm::{ConnectOptions, Database};

mod compact;
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
    store
        .db()
        .execute_unprepared("DROP TRIGGER s75_applied_no_delete")
        .await
        .unwrap();

    let clone = store.clone();
    let warm_work = SharedDecodeBudget::default();
    clone
        .qualify_runtime_schema(DecodeWork::shared(&warm_work))
        .await
        .unwrap();
    assert_eq!(warm_work.lock().unwrap().storage_usage().sql, 0);
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
        .execute_unprepared("UPDATE pin_management_readiness SET state='backfill' WHERE id=1")
        .await
        .unwrap();
    assert!(tx.check_schema(DecodeWork::shared(&work)).await.is_err());
    tx.rollback().await.unwrap();

    store
        .db()
        .execute_unprepared("UPDATE pin_management_readiness SET state='backfill' WHERE id=1")
        .await
        .unwrap();
    let work = SharedDecodeBudget::default();
    let tx = store
        .begin_runtime(DecodeWork::shared(&work))
        .await
        .unwrap();
    tx.db()
        .execute_unprepared("UPDATE pin_management_readiness SET state='ready' WHERE id=1")
        .await
        .unwrap();
    tx.check_schema(DecodeWork::shared(&work)).await.unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn warm_checks_read_live_versions_and_readiness() {
    let store = fixture().await;
    qualify(&store).await.unwrap();
    for sql in [
        "DELETE FROM seaql_migrations WHERE version=(SELECT max(version) FROM seaql_migrations)",
        "UPDATE pin_management_readiness SET state='backfill' WHERE id=1",
    ] {
        let work = SharedDecodeBudget::default();
        let tx = store
            .begin_runtime(DecodeWork::shared(&work))
            .await
            .unwrap();
        tx.db().execute_unprepared(sql).await.unwrap();
        assert!(tx.check_schema(DecodeWork::shared(&work)).await.is_err());
        tx.rollback().await.unwrap();
    }
    let work = SharedDecodeBudget::default();
    let tx = store
        .begin_runtime(DecodeWork::shared(&work))
        .await
        .unwrap();
    tx.check_schema(DecodeWork::shared(&work)).await.unwrap();
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn warm_guard_charges_two_actual_integer_results_and_returns_known_versions() {
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
    assert_eq!(after.sql - before.sql, 2);
    assert_eq!(after.rows - before.rows, 2);
    assert_eq!(after.cells - before.cells, 4);
    assert_eq!(after.raw_bytes - before.raw_bytes, 32);
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn warm_checks_reject_actual_transaction_temp_shadows_before_reading_them() {
    let store = fixture().await;
    qualify(&store).await.unwrap();
    for name in [
        "seaql_migrations",
        "pin_management_readiness",
        "pin_management_archive_identity",
        "pin_legacy_migrations",
        "pin_legacy_migration_items",
        "pin_provider_routes",
        "objects",
        "OBJECTS",
    ] {
        let work = SharedDecodeBudget::default();
        let tx = store
            .begin_runtime(DecodeWork::shared(&work))
            .await
            .unwrap();
        tx.db()
            .execute_unprepared(&format!(
                "CREATE TEMP VIEW {name} AS SELECT json_extract('not json','$') AS invalid"
            ))
            .await
            .unwrap();
        assert!(matches!(
            tx.check_schema(DecodeWork::shared(&work)).await,
            Err(AppError::InvalidPinningRequest(_))
        ));
        tx.rollback().await.unwrap();
    }
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
