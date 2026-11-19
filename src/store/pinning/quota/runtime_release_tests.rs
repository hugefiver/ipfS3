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
    let now = Utc::now();
    // A wrong request ID exercises the same release reads up to the request guard,
    // leaving the correct request's usage lookup outside the allowance.
    let baseline = SharedDecodeBudget::default();
    let work = DecodeWork::shared(&baseline);
    let tx = store.begin_runtime(work).await.unwrap();
    assert_eq!(
        confirmed_runtime_release_for_request(
            &tx,
            "pinata",
            "bafy-release",
            1,
            Some("other-request"),
            now,
            work,
        )
        .await
        .unwrap(),
        ConfirmedReleaseOutcome::Stale
    );
    let prefix_bytes = baseline.lock().unwrap().task_usage().unwrap().raw_bytes;
    tx.rollback().await.unwrap();

    let prior_work = 4096;
    let session = std::sync::Arc::new(std::sync::Mutex::new(ReadSession::for_task(
        Budget::default(),
        u64::MAX,
        prefix_bytes + prior_work,
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
        now,
        work,
    )
    .await;
    assert!(
        matches!(result, Err(AppError::ManagementBudget)),
        "{result:?}"
    );
    let remote = remote_pin::Entity::find_by_id(("pinata".to_owned(), "bafy-release".to_owned()))
        .one(tx.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remote.status, "reserved");
    assert_eq!(remote.request_id.as_deref(), Some("exact-request"));
    tx.rollback().await.unwrap();
    let usage = read_usage(store.db(), "pinata").await.unwrap().unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (7, 1));
    assert_eq!(
        pin_resource_history::Entity::find()
            .count(store.db())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn runtime_release_confirmed_rpc_delete_unblocks_new_allocation() {
    use sea_orm::TransactionTrait;
    let store = fixture(true).await;
    let db = store.db();
    let cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    let identity =
        crate::pinning::identity::ProviderIdentity::legacy("rpc", "ipfs", None, "kubo", "cid");
    super::super::ledger::register_route(db, "rpc", &identity)
        .await
        .unwrap();
    let limits = [(
        "rpc".into(),
        ProviderLimits {
            priority: 1,
            max_bytes: 7,
            max_pins: 1,
            enabled: true,
        },
    )]
    .into();
    let now = Utc::now();
    assert_eq!(
        reserve_unique(db, "rpc", cid, 7, &limits, now)
            .await
            .unwrap(),
        ReservationOutcome::Reserved
    );
    db.execute_unprepared("UPDATE remote_pins SET request_id='pinned-request', status='pinned' WHERE provider='rpc'; UPDATE remote_pin_ledger SET effect='cleanup_pending' WHERE provider='rpc'").await.unwrap();
    assert!(matches!(
        reserve_unique(db, "rpc", cid, 7, &limits, now)
            .await
            .unwrap(),
        ReservationOutcome::QuotaWaiting { .. }
    ));
    assert_eq!(
        confirmed_release(db, "rpc", cid, 1, now).await.unwrap(),
        ConfirmedReleaseOutcome::Stale
    );
    let tx = db.begin().await.unwrap();
    assert_eq!(
        confirmed_release_for_request(&tx, "rpc", cid, 1, Some("pinned-request"), now)
            .await
            .unwrap(),
        ConfirmedReleaseOutcome::Released
    );
    tx.commit().await.unwrap();
    assert_eq!(
        read_usage(db, "rpc").await.unwrap().unwrap().reserved_pins,
        0
    );
    assert_eq!(
        reserve_unique(db, "rpc", cid, 7, &limits, now)
            .await
            .unwrap(),
        ReservationOutcome::Reserved
    );
}

#[tokio::test]
async fn runtime_release_keeps_exact_request_epoch_usage_and_history_atomic() {
    use super::super::ledger;
    use crate::{
        pinning::{
            identity::Ownership,
            provider::{ObservedResource, ObservedResourceStatus},
        },
        store::entities::pin_submit_observation,
    };
    use sea_orm::Statement;

    let store = fixture(true).await;
    let db = store.db();
    let now = Utc::now();
    let expected_cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    let reported_cid = "bafkreigh2akiscaildc6ii5zji4bq7kly5k3s7svv6q2wx2nn5rtj5xuu4";
    let limits = [(
        "pinata".into(),
        ProviderLimits {
            priority: 1,
            max_bytes: 14,
            max_pins: 2,
            enabled: true,
        },
    )]
    .into();
    assert_eq!(
        reserve_unique(db, "pinata", reported_cid, 7, &limits, now)
            .await
            .unwrap(),
        ReservationOutcome::Reserved
    );
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "UPDATE remote_pins SET request_id='reported-request' WHERE provider='pinata' AND cid=$1",
        [reported_cid.into()],
    ))
    .await
    .unwrap();
    let row = ledger::get(db, "pinata", reported_cid)
        .await
        .unwrap()
        .unwrap();
    let route = ledger::decode_route(&row).unwrap();
    let reported = ledger::submission::resource_evidence_for_route(
        &route,
        ObservedResource {
            resource_type: route.resource_type(),
            cid: reported_cid.into(),
            request_id: "reported-request".into(),
            status: ObservedResourceStatus::Reported,
            ownership: Ownership::Unknown,
            replica_evidence: None,
        },
    );
    pin_submit_observation::Entity::insert(pin_submit_observation::ActiveModel {
        id: Set("unresolved".into()),
        job_id: Set("earlier".into()),
        submit_call: Set(1),
        provider: Set("pinata".into()),
        expected_cid: Set(expected_cid.into()),
        route: Set(row.route.unwrap()),
        remote_epoch: Set(1),
        claim_until: Set(now),
        effect: Set("unknown".into()),
        resources: Set(serde_json::to_string(&[reported]).unwrap()),
        outcome: Set("cid_mismatch".into()),
        needs_attention: Set(true),
        started_at: Set(now),
        observed_at: Set(Some(now)),
        ..Default::default()
    })
    .exec(db)
    .await
    .unwrap();

    for (cid, epoch, request, expected) in [
        (
            reported_cid,
            1,
            "reported-request",
            ConfirmedReleaseOutcome::Stale,
        ),
        (
            "bafy-release",
            2,
            "exact-request",
            ConfirmedReleaseOutcome::Stale,
        ),
        (
            "bafy-release",
            1,
            "other-request",
            ConfirmedReleaseOutcome::Stale,
        ),
        (
            "bafy-release",
            1,
            "exact-request",
            ConfirmedReleaseOutcome::Released,
        ),
    ] {
        let session = SharedDecodeBudget::default();
        let work = DecodeWork::shared(&session);
        let tx = store.begin_runtime(work).await.unwrap();
        work.with(|session| session.consume(2, 128)).unwrap();
        assert_eq!(
            confirmed_runtime_release_for_request(
                &tx,
                "pinata",
                cid,
                epoch,
                Some(request),
                now,
                work
            )
            .await
            .unwrap(),
            expected
        );
        if expected == ConfirmedReleaseOutcome::Released {
            tx.rollback().await.unwrap();
            let usage = read_usage(db, "pinata").await.unwrap().unwrap();
            assert_eq!((usage.reserved_bytes, usage.reserved_pins), (14, 2));
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
            now,
            work
        )
        .await
        .unwrap(),
        ConfirmedReleaseOutcome::Released
    );
    tx.commit().await.unwrap();
    let usage = read_usage(db, "pinata").await.unwrap().unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (7, 1));
    let reported = remote_pin::Entity::find_by_id(("pinata".to_owned(), reported_cid.to_owned()))
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reported.status, "reserved");
    assert_eq!(reported.request_id.as_deref(), Some("reported-request"));
    assert_eq!(
        pin_resource_history::Entity::find()
            .count(db)
            .await
            .unwrap(),
        1
    );
}
