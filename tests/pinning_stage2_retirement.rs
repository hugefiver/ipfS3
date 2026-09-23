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
            pin_invocation_route, pin_job, pin_lease_target, pin_provider_route, remote_pin,
        },
        pinning::{
            ledger,
            publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
        },
    },
};
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Set};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

fn runtime(endpoint: &str, retired: bool) -> Arc<PinningCoordinator> {
    let config: Config = toml::from_str(&format!(
        "[pinning_identity]\nprimary_storage_domain='local'\n[[pinning_identity.providers]]\nconfig_name='provider'\nprovider_id='stable'\ndisplay_name='Provider'\nbackend='filebase'\nscope='test-account'\nstorage_domain='remote'\ncredential_revision=1\nendpoint_revision=1\nsecret_ref='env:STAGE2_TOKEN'\napi_profile='filebase-psa'\nstrategy='cid'\nretired={retired}\ncleanup='managed'\n[[pinning.providers]]\nname='provider'\nkind='filebase'\ntoken_env='STAGE2_TOKEN'\nendpoint='{endpoint}'\npriority=1\nmax_bytes=1000\nmax_pins=10\n[pinning]\nworker_interval='1s'\n"
    )).unwrap();
    PinningCoordinator::build(
        ValidatedPinningConfig::from_config(&config, |_| Some("test-token".into())).unwrap(),
    )
    .unwrap()
}

async fn publish(
    store: &Store,
    runtime: &PinningCoordinator,
    key: &str,
    cid: &str,
) -> ipfs_s3_gateway::error::AppResult<publication::PublicationResult> {
    let provider = runtime.provider_limits().keys().next().unwrap().clone();
    publication::publish_object(
        store.db(),
        PublicationRequest {
            object: PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                "retirement",
                key,
                cid.into(),
                100,
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
                    policy_id: "test".into(),
                    provider_mode: ProviderMode::All,
                    providers: vec![provider],
                    content_mode: ContentMode::Object,
                    duration: LeaseDuration::parse("1h").unwrap(),
                }],
            },
            object_target: PinTargetSpec {
                cid: cid.into(),
                logical_size: 100,
            },
        },
        runtime.provider_limits(),
    )
    .await
}

async fn old_queued_poll_converges_after_route_change(retired: bool, observed_status: &str) {
    let server = MockServer::start().await;
    let cid = "bafy-retired-observation";
    let request_id = "old-request";
    Mock::given(method("POST"))
        .and(path("/pins"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"requestid": request_id, "status": "queued", "pin": {"cid": cid}}),
        ))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/pins/{request_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"requestid": request_id, "status": observed_status, "pin": {"cid": cid}}),
        ))
        .expect(1)
        .mount(&server)
        .await;

    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "retirement", None)
        .await
        .unwrap();
    let store = Store::new(db);
    let original = runtime(&server.uri(), false);
    original.register_identities(&store).await.unwrap();
    let provider = original.provider_limits().keys().next().unwrap().clone();
    publish(&store, &original, "old", cid).await.unwrap();
    let worker = original.start(store.clone(), CancellationToken::new());
    tokio::time::timeout(Duration::from_secs(9), async {
        loop {
            if pin_job::Entity::find()
                .filter(pin_job::Column::Operation.eq("poll"))
                .one(store.db())
                .await
                .unwrap()
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    worker.shutdown(Duration::from_secs(5)).await;
    let poll = pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("poll"))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let capture = pin_invocation_route::Entity::find_by_id(&poll.id)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let historical = ledger::get(store.db(), &provider, cid)
        .await
        .unwrap()
        .unwrap()
        .route
        .unwrap();
    assert_eq!(capture.route, historical);

    let runner = if retired {
        let retired_runtime = runtime(&server.uri(), true);
        retired_runtime.register_identities(&store).await.unwrap();
        retired_runtime
    } else {
        // Simulate a route registration changing strategy while the old worker is in flight.
        // Filebase only exposes one configured PSA strategy, so retain its historical worker.
        let row = pin_provider_route::Entity::find_by_id(&provider)
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
        let mut changed: serde_json::Value = serde_json::from_str(&row.snapshot).unwrap();
        changed["strategy"] = serde_json::json!("upload");
        let mut update: pin_provider_route::ActiveModel = row.into();
        update.snapshot = Set(changed.to_string());
        update.update(store.db()).await.unwrap();
        original.clone()
    };
    assert!(
        publish(&store, &runner, "new", cid).await.is_err(),
        "changed route must not attach to the old CID"
    );
    assert_eq!(
        pin_lease_target::Entity::find()
            .all(store.db())
            .await
            .unwrap()
            .len(),
        1
    );

    let worker = runner.start(store.clone(), CancellationToken::new());
    let converged = tokio::time::timeout(Duration::from_secs(9), async {
        loop {
            let remote = remote_pin::Entity::find_by_id((provider.clone(), cid.to_owned()))
                .one(store.db())
                .await
                .unwrap()
                .unwrap();
            if remote.status == observed_status {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    let remote = remote_pin::Entity::find_by_id((provider.clone(), cid.to_owned()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let target = pin_lease_target::Entity::find()
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let poll = pin_job::Entity::find_by_id(&poll.id)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert!(
        converged.is_ok(),
        "old GET observation rolled back after route change: remote={remote:?}, poll={poll:?}"
    );
    assert_eq!(poll.state, "done");
    assert_eq!(
        target.state,
        if observed_status == "pinned" {
            "pinned"
        } else {
            "degraded"
        }
    );
    assert_eq!(
        pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("reconcile"))
            .all(store.db())
            .await
            .unwrap()
            .len(),
        0,
        "historical observation must not create fresh retry work"
    );
    assert_eq!(
        pin_lease_target::Entity::find()
            .all(store.db())
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        ledger::get(store.db(), &provider, cid)
            .await
            .unwrap()
            .unwrap()
            .route
            .as_deref(),
        Some(historical.as_str()),
        "observation must not rebind historical scope"
    );
    assert_eq!(
        pin_invocation_route::Entity::find_by_id(&poll.id)
            .one(store.db())
            .await
            .unwrap()
            .unwrap(),
        capture
    );
    server.verify().await;
}

#[tokio::test]
async fn retired_provider_old_queued_get_pinned_remains_historical_without_new_submit() {
    old_queued_poll_converges_after_route_change(true, "pinned").await;
}

#[tokio::test]
async fn changed_strategy_old_queued_get_pinned_remains_historical_without_new_submit() {
    old_queued_poll_converges_after_route_change(false, "pinned").await;
}

#[tokio::test]
async fn retired_provider_failed_get_records_history_without_scheduling_new_retry() {
    old_queued_poll_converges_after_route_change(true, "failed").await;
}
