use super::*;
use crate::store::pinning::management::{
    native::meter::{Budget, Limits},
    records::ReadSession,
    transaction::CancellationToken,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

async fn fixture() -> DatabaseConnection {
    let mut options = sea_orm::ConnectOptions::new("sqlite::memory:");
    options.max_connections(1);
    let db = sea_orm::Database::connect(options).await.unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    db.execute_unprepared("INSERT INTO remote_pin_ledger(provider,cid,ownership,effect) VALUES ('provider','cid','unknown','confirmed')").await.unwrap();
    db
}

async fn writer_available(db: &DatabaseConnection) {
    tokio::time::timeout(Duration::from_secs(2), async {
        let tx = db.begin().await.unwrap();
        tx.execute_unprepared("UPDATE main.pin_provider_routes SET provider=provider WHERE 1=0")
            .await
            .unwrap();
        tx.rollback().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn completed_observation_releases_connection_and_next_phase_checks_readiness() {
    let db = fixture().await;
    let (cleanup, snapshot) = observe_failed_cleanup(
        &db,
        "provider",
        "cid",
        records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    assert!(!cleanup);
    assert!(snapshot.is_none());
    writer_available(&db).await;
    db.execute_unprepared("UPDATE pin_management_readiness SET state='backfill' WHERE id=1")
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    assert!(
        ledger::cleanup_allowed(&tx, "provider", "cid")
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    assert!(
        observe_failed_cleanup(
            &db,
            "provider",
            "cid",
            records::SharedDecodeBudget::default()
        )
        .await
        .is_err()
    );
    writer_available(&db).await;
}

#[tokio::test]
async fn budget_failure_and_cancellation_release_observation_transaction() {
    let db = fixture().await;
    for cancelled in [false, true] {
        let cancellation = CancellationToken::new();
        if cancelled {
            cancellation.cancel();
        }
        let work = Arc::new(Mutex::new(ReadSession::for_task(
            Budget::new(Limits {
                sql: 0,
                ..Limits::default()
            }),
            u64::MAX,
            u64::MAX,
            cancellation,
        )));
        assert!(
            observe_failed_cleanup(&db, "provider", "cid", work)
                .await
                .is_err()
        );
        writer_available(&db).await;
    }
    let window = WorkerReadWindow::begin(&db).await.unwrap();
    drop(window);
    writer_available(&db).await;
}

#[tokio::test]
async fn next_observation_rejects_changed_catalog_and_temp_route_shadow() {
    let db = fixture().await;
    observe_failed_cleanup(
        &db,
        "provider",
        "cid",
        records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    db.execute_unprepared("CREATE TEMP TABLE pin_provider_routes(provider TEXT)")
        .await
        .unwrap();
    assert!(
        observe_failed_cleanup(
            &db,
            "provider",
            "cid",
            records::SharedDecodeBudget::default()
        )
        .await
        .is_err()
    );
    writer_available(&db).await;
    db.execute_unprepared("DROP TABLE temp.pin_provider_routes; DROP TABLE pin_provider_routes; CREATE TABLE pin_provider_routes(provider TEXT)").await.unwrap();
    assert!(
        observe_failed_cleanup(
            &db,
            "provider",
            "cid",
            records::SharedDecodeBudget::default()
        )
        .await
        .is_err()
    );
    writer_available(&db).await;
}

#[tokio::test]
async fn store_observations_reload_business_rows() {
    let store = crate::store::Store::new(fixture().await);
    let observe = || {
        observe_failed_cleanup_store(
            &store,
            "provider",
            "cid",
            records::SharedDecodeBudget::default(),
        )
    };
    assert!(!observe().await.unwrap().0);
    assert!(!observe().await.unwrap().0);
    writer_available(store.db()).await;
    store
        .db()
        .execute_unprepared("DELETE FROM remote_pin_ledger WHERE provider='provider' AND cid='cid'")
        .await
        .unwrap();
    assert!(observe().await.unwrap().0);
    writer_available(store.db()).await;
}
