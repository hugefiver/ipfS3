use std::{sync::Arc, time::Duration};

use chrono::Utc;
use ipfs_s3_gateway::{
    config::Config,
    pinning::{
        config::{LeaseDuration, ProviderMode, ValidatedPinningConfig},
        coordinator::PinningCoordinator,
        policy::{LeaseIntent, LeaseSource, PublicationPolicy},
        tags::ContentMode,
    },
    store::{
        self, Store,
        entities::{
            pin_invocation_route, pin_job, pin_lease, pin_lease_target, pin_provider_route,
            remote_pin, remote_pin_ledger,
        },
        pinning::{
            jobs, leases, ledger,
            publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
            quota,
        },
    },
};
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Set, TransactionTrait};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const CID: &str = "bafy-stage2-queued-reattach";
const REQUEST: &str = "queued-request";

async fn fixture(
    server: &MockServer,
) -> (tempfile::TempDir, Store, Arc<PinningCoordinator>, String) {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("queued-reattach.sqlite")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let db = store::connect_database(&url).await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "queued-reattach", None)
        .await
        .unwrap();
    let config: Config = toml::from_str(&format!(
        "[pinning_identity]\nprimary_storage_domain='local'\n[[pinning_identity.providers]]\nconfig_name='provider'\nprovider_id='stable'\ndisplay_name='Provider'\nbackend='filebase'\nscope='test-account'\nstorage_domain='remote'\ncredential_revision=1\nendpoint_revision=1\nsecret_ref='env:STAGE2_TOKEN'\napi_profile='filebase-psa'\nstrategy='cid'\ncleanup='retain'\n[[pinning.providers]]\nname='provider'\nkind='filebase'\ntoken_env='STAGE2_TOKEN'\nendpoint='{}'\npriority=1\nmax_bytes=1000\nmax_pins=1\n[pinning]\nworker_interval='1s'\n",
        server.uri()
    )).unwrap();
    let runtime = PinningCoordinator::build(
        ValidatedPinningConfig::from_config(&config, |_| Some("test-token".into())).unwrap(),
    )
    .unwrap();
    let store = Store::new(db);
    runtime.register_identities(&store).await.unwrap();
    let provider = runtime.provider_limits().keys().next().unwrap().clone();
    (directory, store, runtime, provider)
}

async fn publish(store: &Store, runtime: &PinningCoordinator, provider: &str, key: &str) {
    publication::publish_object(
        store.db(),
        PublicationRequest {
            object: PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                "queued-reattach",
                key,
                CID.into(),
                80,
                None,
                None,
                false,
                None,
                None,
                Utc::now(),
            ),
            tags: vec![],
            policy: PublicationPolicy {
                tags: vec![],
                leases: vec![LeaseIntent {
                    source: LeaseSource::Automatic,
                    policy_id: "queued-reattach".into(),
                    provider_mode: ProviderMode::All,
                    providers: vec![provider.into()],
                    content_mode: ContentMode::Object,
                    duration: LeaseDuration::parse("1h").unwrap(),
                }],
            },
            object_target: PinTargetSpec {
                cid: CID.into(),
                logical_size: 80,
            },
        },
        runtime.provider_limits(),
    )
    .await
    .unwrap();
}

async fn wait_for<F, Fut>(mut ready: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    tokio::time::timeout(Duration::from_secs(12), async {
        while !ready().await {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("pinning state did not converge");
}

async fn request_counts(server: &MockServer) -> (usize, usize) {
    let requests = server.received_requests().await.unwrap();
    (
        requests
            .iter()
            .filter(|r| r.method.as_str() == "POST")
            .count(),
        requests
            .iter()
            .filter(|r| r.method.as_str() == "GET")
            .count(),
    )
}

#[tokio::test]
async fn queued_retained_reattach_requires_historical_get_before_confirmation() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/pins"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "requestid": REQUEST, "status": "queued", "pin": {"cid": CID}
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/pins/{REQUEST}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "requestid": REQUEST, "status": "pinned", "pin": {"cid": CID}
        })))
        .expect(1)
        .mount(&server)
        .await;

    let (_directory, store, runtime, provider) = fixture(&server).await;
    publish(&store, &runtime, &provider, "first").await;
    let worker = runtime.start(store.clone(), CancellationToken::new());
    wait_for(|| async {
        pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(store.db())
            .await
            .unwrap()
            .is_some_and(|poll| poll.state == "pending")
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    assert_eq!(request_counts(&server).await, (1, 0));

    let old_lease = pin_lease::Entity::find()
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let old_poll = pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("poll"))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let captured = pin_invocation_route::Entity::find_by_id(&old_poll.id)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let old_remote = remote_pin::Entity::find_by_id((provider.clone(), CID.into()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old_remote.status, "queued");
    assert_eq!(old_remote.request_id.as_deref(), Some(REQUEST));
    assert_eq!(captured.remote_epoch, old_remote.epoch);
    let previous_pin_time = Utc::now() - chrono::Duration::hours(1);
    let row = ledger::get(store.db(), &provider, CID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.effect, "unknown");
    assert!(row.first_observed_at.is_some());
    // Existing historical evidence may predate the queued response; observing a
    // later GET must not replace the first timestamps or established ownership.
    let mut seeded: remote_pin_ledger::ActiveModel = row.into();
    seeded.ownership = Set("external_existing".into());
    seeded.remote_pinned_at = Set(Some(previous_pin_time));
    seeded.update(store.db()).await.unwrap();

    let txn = store.db().begin().await.unwrap();
    leases::cancel_lease(&txn, &old_lease.id, Utc::now())
        .await
        .unwrap();
    txn.commit().await.unwrap();
    let worker = runtime.start(store.clone(), CancellationToken::new());
    wait_for(|| async {
        ledger::get(store.db(), &provider, CID)
            .await
            .unwrap()
            .unwrap()
            .effect
            == "retained"
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    assert_eq!(request_counts(&server).await, (1, 0));
    let retained = ledger::get(store.db(), &provider, CID)
        .await
        .unwrap()
        .unwrap();
    let retained_remote = remote_pin::Entity::find_by_id((provider.clone(), CID.into()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained_remote.status, "queued");
    assert_eq!(retained_remote.request_id.as_deref(), Some(REQUEST));
    assert_eq!(retained.ownership, "external_existing");
    let usage = quota::read_usage(store.db(), &provider)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_pins, usage.reserved_bytes), (1, 80));
    assert!(
        !ledger::lease_status(store.db(), &old_lease.id)
            .await
            .unwrap()
            .unwrap()
            .any_provider_available
    );

    publish(&store, &runtime, &provider, "second").await;
    let new_lease = pin_lease::Entity::find()
        .filter(pin_lease::Column::State.eq("active"))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(new_lease.id, old_lease.id);
    let new_remote = remote_pin::Entity::find_by_id((provider.clone(), CID.into()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(new_remote.status, "queued");
    assert_eq!(new_remote.epoch, retained_remote.epoch + 1);
    assert_eq!(new_remote.request_id.as_deref(), Some(REQUEST));
    assert_eq!(captured.route, retained.route.as_deref().unwrap());
    assert_eq!(
        ledger::get(store.db(), &provider, CID)
            .await
            .unwrap()
            .unwrap()
            .effect,
        "retained"
    );
    assert!(
        !ledger::lease_status(store.db(), &new_lease.id)
            .await
            .unwrap()
            .unwrap()
            .any_provider_available
    );
    assert_eq!(
        request_counts(&server).await,
        (1, 0),
        "reattach must not issue another POST or an early GET"
    );

    let worker = runtime.start(store.clone(), CancellationToken::new());
    wait_for(|| async {
        pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq(&new_lease.id))
            .one(store.db())
            .await
            .unwrap()
            .is_some_and(|target| target.state == "pinned")
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    assert_eq!(request_counts(&server).await, (1, 1));
    let status = ledger::lease_status(store.db(), &new_lease.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        status.any_provider_available,
        "real historical GET must restore availability"
    );
    assert!(status.all_targets_pinned);
    let after = ledger::get(store.db(), &provider, CID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.effect, "confirmed");
    assert_eq!(after.ownership, retained.ownership);
    assert_eq!(after.first_observed_at, retained.first_observed_at);
    assert_eq!(after.remote_pinned_at, retained.remote_pinned_at);
    assert!(after.last_observed_at > retained.last_observed_at);
    let usage = quota::read_usage(store.db(), &provider)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_pins, usage.reserved_bytes), (1, 80));
    assert!(
        !ledger::lease_status(store.db(), &old_lease.id)
            .await
            .unwrap()
            .unwrap()
            .any_provider_available
    );
}

#[tokio::test]
async fn pinned_persisted_replay_cannot_confirm_a_queued_retained_reattach() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/pins"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "requestid": REQUEST, "status": "queued", "pin": {"cid": CID}
        })))
        .expect(1)
        .mount(&server)
        .await;
    let (_directory, store, runtime, provider) = fixture(&server).await;
    publish(&store, &runtime, &provider, "first").await;
    let worker = runtime.start(store.clone(), CancellationToken::new());
    wait_for(|| async {
        pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(store.db())
            .await
            .unwrap()
            .is_some()
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    let lease = pin_lease::Entity::find()
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let txn = store.db().begin().await.unwrap();
    leases::cancel_lease(&txn, &lease.id, Utc::now())
        .await
        .unwrap();
    txn.commit().await.unwrap();
    let worker = runtime.start(store.clone(), CancellationToken::new());
    wait_for(|| async {
        ledger::get(store.db(), &provider, CID)
            .await
            .unwrap()
            .unwrap()
            .effect
            == "retained"
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    publish(&store, &runtime, &provider, "second").await;
    let active = pin_lease::Entity::find()
        .filter(pin_lease::Column::State.eq("active"))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    pin_job::Entity::update_many()
        .col_expr(
            pin_job::Column::State,
            sea_orm::sea_query::Expr::value("done"),
        )
        .filter(pin_job::Column::Operation.eq("poll"))
        .exec(store.db())
        .await
        .unwrap();
    remote_pin::Entity::update_many()
        .col_expr(
            remote_pin::Column::Status,
            sea_orm::sea_query::Expr::value("pinned"),
        )
        .filter(remote_pin::Column::Provider.eq(&provider))
        .filter(remote_pin::Column::Cid.eq(CID))
        .exec(store.db())
        .await
        .unwrap();
    let remote = remote_pin::Entity::find_by_id((provider.clone(), CID.into()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let jobs::NewPinJob::Remote(reconcile) =
        jobs::reconcile_job(&provider, CID, remote.epoch, Utc::now())
    else {
        unreachable!()
    };
    let id = reconcile.id.clone();
    jobs::enqueue_job(store.db(), jobs::NewPinJob::Remote(reconcile))
        .await
        .unwrap();
    let worker = runtime.start(store.clone(), CancellationToken::new());
    wait_for(|| async {
        pin_job::Entity::find_by_id(&id)
            .one(store.db())
            .await
            .unwrap()
            .is_some_and(|job| job.state == "done")
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    assert_eq!(request_counts(&server).await, (1, 0));
    assert_eq!(
        ledger::get(store.db(), &provider, CID)
            .await
            .unwrap()
            .unwrap()
            .effect,
        "retained"
    );
    assert!(
        !ledger::lease_status(store.db(), &active.id)
            .await
            .unwrap()
            .unwrap()
            .any_provider_available
    );
    let usage = quota::read_usage(store.db(), &provider)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_pins, usage.reserved_bytes), (1, 80));
}

#[tokio::test]
async fn pinned_get_after_last_reference_ends_leaves_retained_capacity() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/pins"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "requestid": REQUEST, "status": "queued", "pin": {"cid": CID}
        })))
        .expect(1)
        .mount(&server)
        .await;
    let get_started = Arc::new(tokio::sync::Notify::new());
    let signal = get_started.clone();
    Mock::given(method("GET"))
        .and(path(format!("/pins/{REQUEST}")))
        .respond_with(move |_: &wiremock::Request| {
            signal.notify_one();
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(750))
                .set_body_json(serde_json::json!({
                    "requestid": REQUEST, "status": "pinned", "pin": {"cid": CID}
                }))
        })
        .expect(1)
        .mount(&server)
        .await;
    let (_directory, store, runtime, provider) = fixture(&server).await;
    publish(&store, &runtime, &provider, "first").await;
    let worker = runtime.start(store.clone(), CancellationToken::new());
    wait_for(|| async {
        pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(store.db())
            .await
            .unwrap()
            .is_some()
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    let first = pin_lease::Entity::find()
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let txn = store.db().begin().await.unwrap();
    leases::cancel_lease(&txn, &first.id, Utc::now())
        .await
        .unwrap();
    txn.commit().await.unwrap();
    let worker = runtime.start(store.clone(), CancellationToken::new());
    wait_for(|| async {
        ledger::get(store.db(), &provider, CID)
            .await
            .unwrap()
            .unwrap()
            .effect
            == "retained"
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    publish(&store, &runtime, &provider, "second").await;
    let second = pin_lease::Entity::find()
        .filter(pin_lease::Column::State.eq("active"))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request_counts(&server).await, (1, 0));

    let worker = runtime.start(store.clone(), CancellationToken::new());
    tokio::time::timeout(Duration::from_secs(10), get_started.notified())
        .await
        .unwrap();
    let txn = store.db().begin().await.unwrap();
    leases::cancel_lease(&txn, &second.id, Utc::now())
        .await
        .unwrap();
    txn.commit().await.unwrap();
    wait_for(|| async {
        remote_pin::Entity::find_by_id((provider.clone(), CID.into()))
            .one(store.db())
            .await
            .unwrap()
            .is_some_and(|remote| remote.status == "pinned")
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    assert_eq!(request_counts(&server).await, (1, 1));
    assert_eq!(
        ledger::get(store.db(), &provider, CID)
            .await
            .unwrap()
            .unwrap()
            .effect,
        "retained"
    );
    assert!(
        !ledger::lease_status(store.db(), &second.id)
            .await
            .unwrap()
            .unwrap()
            .any_provider_available
    );
    let usage = quota::read_usage(store.db(), &provider)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_pins, usage.reserved_bytes), (1, 80));
}

#[tokio::test]
async fn route_revision_during_historical_get_cannot_confirm_retained_pin() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/pins"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "requestid": REQUEST, "status": "queued", "pin": {"cid": CID}
        })))
        .expect(1)
        .mount(&server)
        .await;
    let get_started = Arc::new(tokio::sync::Notify::new());
    let signal = get_started.clone();
    Mock::given(method("GET"))
        .and(path(format!("/pins/{REQUEST}")))
        .respond_with(move |_: &wiremock::Request| {
            signal.notify_one();
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(750))
                .set_body_json(serde_json::json!({
                    "requestid": REQUEST, "status": "pinned", "pin": {"cid": CID}
                }))
        })
        .expect(1)
        .mount(&server)
        .await;
    let (_directory, store, runtime, provider) = fixture(&server).await;
    publish(&store, &runtime, &provider, "first").await;
    let worker = runtime.start(store.clone(), CancellationToken::new());
    wait_for(|| async {
        pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(store.db())
            .await
            .unwrap()
            .is_some()
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    let first = pin_lease::Entity::find()
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let txn = store.db().begin().await.unwrap();
    leases::cancel_lease(&txn, &first.id, Utc::now())
        .await
        .unwrap();
    txn.commit().await.unwrap();
    let worker = runtime.start(store.clone(), CancellationToken::new());
    wait_for(|| async {
        ledger::get(store.db(), &provider, CID)
            .await
            .unwrap()
            .unwrap()
            .effect
            == "retained"
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    publish(&store, &runtime, &provider, "second").await;
    let second = pin_lease::Entity::find()
        .filter(pin_lease::Column::State.eq("active"))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request_counts(&server).await, (1, 0));

    let worker = runtime.start(store.clone(), CancellationToken::new());
    tokio::time::timeout(Duration::from_secs(10), get_started.notified())
        .await
        .unwrap();
    let registered = pin_provider_route::Entity::find_by_id(&provider)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let mut changed: serde_json::Value = serde_json::from_str(&registered.snapshot).unwrap();
    changed["credential_revision"] = serde_json::json!(2);
    let mut revision: pin_provider_route::ActiveModel = registered.into();
    revision.snapshot = Set(changed.to_string());
    revision.update(store.db()).await.unwrap();
    wait_for(|| async {
        remote_pin::Entity::find_by_id((provider.clone(), CID.into()))
            .one(store.db())
            .await
            .unwrap()
            .is_some_and(|remote| remote.status == "pinned")
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    assert_eq!(request_counts(&server).await, (1, 1));
    assert_eq!(
        ledger::get(store.db(), &provider, CID)
            .await
            .unwrap()
            .unwrap()
            .effect,
        "retained"
    );
    assert!(
        !ledger::lease_status(store.db(), &second.id)
            .await
            .unwrap()
            .unwrap()
            .any_provider_available
    );
    let usage = quota::read_usage(store.db(), &provider)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_pins, usage.reserved_bytes), (1, 80));
}
