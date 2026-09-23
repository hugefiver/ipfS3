use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
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
        entities::{pin_job, pin_lease_target, remote_pin, remote_pin_ledger},
        pinning::{
            jobs, ledger,
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

const CID: &str = "bafy-observation-replay";
const REQUEST: &str = "existing-request";

async fn setup(server: &MockServer) -> (Store, Arc<PinningCoordinator>, String) {
    let config: Config = toml::from_str(&format!(
        "[pinning_identity]\nprimary_storage_domain='local'\n[[pinning_identity.providers]]\nconfig_name='provider'\nprovider_id='stable'\ndisplay_name='Provider'\nbackend='filebase'\nscope='test-account'\nstorage_domain='remote'\ncredential_revision=1\nendpoint_revision=1\nsecret_ref='env:STAGE2_TOKEN'\napi_profile='filebase-psa'\nstrategy='cid'\ncleanup='managed'\n[[pinning.providers]]\nname='provider'\nkind='filebase'\ntoken_env='STAGE2_TOKEN'\nendpoint='{}'\npriority=1\nmax_bytes=1000\nmax_pins=10\n[pinning]\nworker_interval='1s'\n",
        server.uri()
    )).unwrap();
    let runtime = PinningCoordinator::build(
        ValidatedPinningConfig::from_config(&config, |_| Some("test-token".into())).unwrap(),
    )
    .unwrap();
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "observation", None)
        .await
        .unwrap();
    let store = Store::new(db);
    runtime.register_identities(&store).await.unwrap();
    let provider = runtime.provider_limits().keys().next().unwrap().clone();
    publication::publish_object(
        store.db(),
        PublicationRequest {
            object: PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                "observation",
                "key",
                CID.into(),
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
                    providers: vec![provider.clone()],
                    content_mode: ContentMode::Object,
                    duration: LeaseDuration::parse("1h").unwrap(),
                }],
            },
            object_target: PinTargetSpec {
                cid: CID.into(),
                logical_size: 100,
            },
        },
        runtime.provider_limits(),
    )
    .await
    .unwrap();
    // No Submit is due while these tests exercise an already known request.
    pin_job::Entity::update_many()
        .col_expr(
            pin_job::Column::State,
            sea_orm::sea_query::Expr::value("done"),
        )
        .filter(pin_job::Column::Operation.eq("submit"))
        .exec(store.db())
        .await
        .unwrap();
    (store, runtime, provider)
}

async fn seed_remote(
    store: &Store,
    provider: &str,
    status: &str,
    observed: DateTime<Utc>,
) -> remote_pin_ledger::Model {
    let row = remote_pin::Entity::find_by_id((provider.to_owned(), CID.to_owned()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let mut update: remote_pin::ActiveModel = row.into();
    update.request_id = Set(Some(REQUEST.into()));
    update.status = Set(status.into());
    update.update(store.db()).await.unwrap();
    let row = ledger::get(store.db(), provider, CID)
        .await
        .unwrap()
        .unwrap();
    let mut update: remote_pin_ledger::ActiveModel = row.into();
    update.ownership = Set("external_existing".into());
    update.effect = Set(if status == "pinned" {
        "confirmed"
    } else {
        "unknown"
    }
    .into());
    update.first_observed_at = Set(Some(observed));
    update.last_observed_at = Set(Some(observed));
    update.remote_pinned_at = Set((status == "pinned").then_some(observed));
    update.update(store.db()).await.unwrap()
}

async fn wait_done(store: &Store, id: &str) {
    tokio::time::timeout(Duration::from_secs(9), async {
        loop {
            if pin_job::Entity::find_by_id(id.to_owned())
                .one(store.db())
                .await
                .unwrap()
                .is_some_and(|job| job.state == "done")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("worker did not finish the expected job");
}

#[tokio::test]
async fn persisted_reconcile_projects_without_forging_provider_observation() {
    for (status, projected) in [
        ("queued", "submitted"),
        ("pinning", "submitted"),
        ("pinned", "pinned"),
    ] {
        let server = MockServer::start().await;
        let (store, runtime, provider) = setup(&server).await;
        let prior = Utc::now() - chrono::Duration::hours(1);
        let before = seed_remote(&store, &provider, status, prior).await;
        let remote = remote_pin::Entity::find_by_id((provider.clone(), CID.to_owned()))
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
        let job = jobs::reconcile_job(&provider, CID, remote.epoch, Utc::now());
        let jobs::NewPinJob::Remote(job_id) = &job else {
            unreachable!()
        };
        let id = job_id.id.clone();
        jobs::enqueue_job(store.db(), job).await.unwrap();
        let worker = runtime.start(store.clone(), CancellationToken::new());
        wait_done(&store, &id).await;
        worker.shutdown(Duration::from_secs(5)).await;

        assert_eq!(
            ledger::get(store.db(), &provider, CID)
                .await
                .unwrap()
                .unwrap(),
            before,
            "replay of {status} changed observation evidence"
        );
        assert_eq!(
            pin_lease_target::Entity::find()
                .one(store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            projected
        );
        assert!(
            server.received_requests().await.unwrap().is_empty(),
            "replay of {status} called provider"
        );
    }
}

#[tokio::test]
async fn provider_get_advances_observation_and_confirms_pin() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/pins/{REQUEST}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "requestid": REQUEST, "status": "pinned", "pin": {"cid": CID}
        })))
        .expect(1)
        .mount(&server)
        .await;
    let (store, runtime, provider) = setup(&server).await;
    let prior = Utc::now() - chrono::Duration::hours(1);
    let before = seed_remote(&store, &provider, "queued", prior).await;
    let target = pin_lease_target::Entity::find()
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let mut update: pin_lease_target::ActiveModel = target.clone().into();
    update.state = Set("submitted".into());
    update.update(store.db()).await.unwrap();
    let job = jobs::poll_job(
        &provider,
        CID,
        &target.lease_id,
        &target.id,
        1,
        REQUEST,
        Utc::now(),
    );
    let jobs::NewPinJob::Target(job_id) = &job else {
        unreachable!()
    };
    let id = job_id.id.clone();
    jobs::enqueue_job(store.db(), job).await.unwrap();
    let worker = runtime.start(store.clone(), CancellationToken::new());
    wait_done(&store, &id).await;
    worker.shutdown(Duration::from_secs(5)).await;

    let after = ledger::get(store.db(), &provider, CID)
        .await
        .unwrap()
        .unwrap();
    assert!(after.last_observed_at.unwrap() > before.last_observed_at.unwrap());
    assert!(after.remote_pinned_at.unwrap() > prior);
    assert_eq!(after.effect, "confirmed");
    assert_eq!(
        pin_lease_target::Entity::find_by_id(target.id)
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "pinned"
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn provider_post_advances_observation_and_confirms_pin() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/pins"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "requestid": REQUEST, "status": "pinned", "pin": {"cid": CID}
        })))
        .expect(1)
        .mount(&server)
        .await;
    let (store, runtime, provider) = setup(&server).await;
    let prior = Utc::now() - chrono::Duration::hours(1);
    let row = ledger::get(store.db(), &provider, CID)
        .await
        .unwrap()
        .unwrap();
    let mut update: remote_pin_ledger::ActiveModel = row.into();
    update.first_observed_at = Set(Some(prior));
    update.last_observed_at = Set(Some(prior));
    update.update(store.db()).await.unwrap();
    pin_job::Entity::update_many()
        .col_expr(
            pin_job::Column::State,
            sea_orm::sea_query::Expr::value("pending"),
        )
        .filter(pin_job::Column::Operation.eq("submit"))
        .exec(store.db())
        .await
        .unwrap();
    let id = pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("submit"))
        .one(store.db())
        .await
        .unwrap()
        .unwrap()
        .id;
    let worker = runtime.start(store.clone(), CancellationToken::new());
    wait_done(&store, &id).await;
    worker.shutdown(Duration::from_secs(5)).await;

    let after = ledger::get(store.db(), &provider, CID)
        .await
        .unwrap()
        .unwrap();
    assert!(after.last_observed_at.unwrap() > prior);
    assert!(after.remote_pinned_at.unwrap() > prior);
    assert_eq!(after.effect, "confirmed");
    assert_eq!(after.ownership, "unknown");
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}
