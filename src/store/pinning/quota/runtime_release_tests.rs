use super::*;
use crate::store::{
    Store,
    entities::pin_resource_history,
    pinning::management::{
        native::meter::Budget, records::ReadSession, transaction::CancellationToken,
    },
};
use sea_orm::{ConnectOptions, Database, PaginatorTrait};

async fn fixture(registered: bool) -> Store {
    let mut options = ConnectOptions::new("sqlite::memory:");
    options.max_connections(1);
    let db = Database::connect(options).await.unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    if registered {
        super::super::ledger::register_route(
            &db,
            "pinata",
            &crate::pinning::identity::ProviderIdentity::legacy(
                "pinata", "noop", None, "psa", "psa",
            ),
        )
        .await
        .unwrap();
    }
    let limits = [(
        "pinata".into(),
        ProviderLimits {
            priority: 1,
            max_bytes: 100,
            max_pins: 10,
            enabled: true,
        },
    )]
    .into();
    reserve_unique(&db, "pinata", "bafy-release", 7, &limits, Utc::now())
        .await
        .unwrap();
    db.execute_unprepared("UPDATE remote_pins SET request_id='exact-request'")
        .await
        .unwrap();
    let store = Store::new(db);
    store
        .qualify_runtime_schema(DecodeWork::shared(&SharedDecodeBudget::default()))
        .await
        .unwrap();
    store
}

#[tokio::test]
async fn runtime_release_usage_resolution_consumes_the_partially_used_operation_budget() {
    let store = fixture(false).await;
    // Measure the other qualified release reads, leaving only usage resolution
    // outside this allowance. The limit is relative to real work, not SQL text.
    let baseline = SharedDecodeBudget::default();
    let work = DecodeWork::shared(&baseline);
    let tx = store.begin_runtime(work).await.unwrap();
    super::super::ledger::submission::admission_barrier_with_catalog(
        "pinata",
        tx.catalog(work),
        work,
    )
    .await
    .unwrap();
    super::super::ledger::get_catalog(tx.catalog(work), "pinata", "bafy-release", work)
        .await
        .unwrap();
    let remote = remote_pin::Entity::find_by_id(("pinata".to_owned(), "bafy-release".to_owned()))
        .one(tx.db())
        .await
        .unwrap()
        .unwrap();
    super::super::ledger::archive_runtime_confirmed_release(&tx, &remote, work)
        .await
        .unwrap();
    super::super::ledger::mark_runtime_effect(&tx, "pinata", "bafy-release", "absent", work)
        .await
        .unwrap();
    let other_bytes = baseline.lock().unwrap().task_usage().unwrap().raw_bytes;
    tx.rollback().await.unwrap();

    let prior_work = 4096;
    let session = std::sync::Arc::new(std::sync::Mutex::new(ReadSession::for_task(
        Budget::default(),
        u64::MAX,
        other_bytes + prior_work + 64,
        CancellationToken::new(),
    )));
    let work = DecodeWork::shared(&session);
    let tx = store.begin_runtime(work).await.unwrap();
    work.with(|session| session.consume(0, prior_work as usize))
        .unwrap();
    let result = confirmed_runtime_release_for_request(
        &tx,
        "pinata",
        "bafy-release",
        1,
        Some("exact-request"),
        Utc::now(),
        work,
    )
    .await;
    assert!(
        matches!(result, Err(AppError::ManagementBudget)),
        "{result:?}"
    );
    tx.rollback().await.unwrap();
    let usage = read_usage(store.db(), "pinata").await.unwrap().unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (7, 1));
    let remote = remote_pin::Entity::find_by_id(("pinata".to_owned(), "bafy-release".to_owned()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remote.status, "reserved");
    assert_eq!(remote.request_id.as_deref(), Some("exact-request"));
    assert_eq!(
        pin_resource_history::Entity::find()
            .count(store.db())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn runtime_release_keeps_exact_request_epoch_usage_and_history_atomic() {
    let store = fixture(true).await;
    for (epoch, request, expected) in [
        (2, "exact-request", ConfirmedReleaseOutcome::Stale),
        (1, "other-request", ConfirmedReleaseOutcome::Stale),
        (1, "exact-request", ConfirmedReleaseOutcome::Released),
    ] {
        let session = SharedDecodeBudget::default();
        let work = DecodeWork::shared(&session);
        let tx = store.begin_runtime(work).await.unwrap();
        work.with(|session| session.consume(2, 128)).unwrap();
        assert_eq!(
            confirmed_runtime_release_for_request(
                &tx,
                "pinata",
                "bafy-release",
                epoch,
                Some(request),
                Utc::now(),
                work
            )
            .await
            .unwrap(),
            expected
        );
        if expected == ConfirmedReleaseOutcome::Released {
            tx.rollback().await.unwrap();
            assert_eq!(
                read_usage(store.db(), "pinata")
                    .await
                    .unwrap()
                    .unwrap()
                    .reserved_bytes,
                7
            );
            assert_eq!(
                pin_resource_history::Entity::find()
                    .count(store.db())
                    .await
                    .unwrap(),
                0
            );
        } else {
            tx.commit().await.unwrap();
        }
    }
    let session = SharedDecodeBudget::default();
    let work = DecodeWork::shared(&session);
    let tx = store.begin_runtime(work).await.unwrap();
    assert_eq!(
        confirmed_runtime_release_for_request(
            &tx,
            "pinata",
            "bafy-release",
            1,
            Some("exact-request"),
            Utc::now(),
            work
        )
        .await
        .unwrap(),
        ConfirmedReleaseOutcome::Released
    );
    tx.commit().await.unwrap();
    let usage = read_usage(store.db(), "pinata").await.unwrap().unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (0, 0));
    assert_eq!(
        pin_resource_history::Entity::find()
            .count(store.db())
            .await
            .unwrap(),
        1
    );
}
