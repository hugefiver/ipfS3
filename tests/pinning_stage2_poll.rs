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
            pin_invocation_route, pin_job, pin_lease_target, remote_pin, remote_pin_ledger,
        },
        pinning::{
            leases, ledger,
            publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
        },
    },
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Set, TransactionTrait,
};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

fn coordinator(endpoint: &str) -> Arc<PinningCoordinator> {
    let config: Config = toml::from_str(&format!(
        "[pinning_identity]\nprimary_storage_domain='local'\n[[pinning_identity.providers]]\nconfig_name='provider'\nprovider_id='stable'\ndisplay_name='Provider'\nbackend='filebase'\nscope='test-account'\nstorage_domain='remote'\ncredential_revision=1\nendpoint_revision=1\nsecret_ref='env:STAGE2_TOKEN'\napi_profile='filebase-psa'\nstrategy='cid'\ncleanup='managed'\n[[pinning.providers]]\nname='provider'\nkind='filebase'\ntoken_env='STAGE2_TOKEN'\nendpoint='{endpoint}'\npriority=1\nmax_bytes=1000\nmax_pins=10\n[pinning]\nworker_interval='1s'\n"
    ))
    .unwrap();
    PinningCoordinator::build(
        ValidatedPinningConfig::from_config(&config, |_| Some("test-token".into())).unwrap(),
    )
    .unwrap()
}

async fn published_store(endpoint: &str, cid: &str) -> (Store, Arc<PinningCoordinator>, String) {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "poll", None).await.unwrap();
    let store = Store::new(db);
    let runtime = coordinator(endpoint);
    runtime.register_identities(&store).await.unwrap();
    let provider = runtime.provider_limits().keys().next().unwrap().clone();
    publication::publish_object(
        store.db(),
        PublicationRequest {
            object: PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                "poll",
                "key",
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
                    providers: vec![provider.clone()],
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
    .unwrap();
    (store, runtime, provider)
}

async fn poll(store: &Store) -> pin_job::Model {
    pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("poll"))
        .one(store.db())
        .await
        .unwrap()
        .expect("Submit must schedule Poll")
}

async fn captured(store: &Store, id: &str) -> Option<pin_invocation_route::Model> {
    pin_invocation_route::Entity::find_by_id(id)
        .one(store.db())
        .await
        .unwrap()
}

async fn project(store: &Store, target_id: &str) -> bool {
    let txn = store.db().begin().await.unwrap();
    let result = leases::project_target_from_remote(&txn, target_id, Utc::now()).await;
    if result.is_ok() {
        txn.commit().await.unwrap();
    } else {
        txn.rollback().await.unwrap();
    }
    result.is_ok()
}

#[tokio::test]
async fn prelocked_publication_queued_submit_poll_reaches_pinned() {
    let server = MockServer::start().await;
    let cid = "bafy-prelocked-poll";
    let request_id = "stage2-request";
    Mock::given(method("POST"))
        .and(path("/pins"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "requestid": request_id, "status": "queued", "pin": {"cid": cid}
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/pins/{request_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "requestid": request_id, "status": "pinned", "pin": {"cid": cid}
        })))
        .expect(1)
        .mount(&server)
        .await;

    let (store, runtime, provider) = published_store(&server.uri(), cid).await;
    let worker = runtime.start(store.clone(), CancellationToken::new());
    let converged = tokio::time::timeout(Duration::from_secs(9), async {
        loop {
            let remote = remote_pin::Entity::find_by_id((provider.clone(), cid.to_owned()))
                .one(store.db())
                .await
                .unwrap()
                .unwrap();
            if remote.status == "pinned" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;

    let poll = poll(&store).await;
    let remote = remote_pin::Entity::find_by_id((provider, cid.to_owned()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert!(
        converged.is_ok(),
        "queued request did not reach pinned: remote={remote:?}, poll={poll:?}"
    );
    assert_eq!(poll.state, "done");
    let capture = captured(&store, &poll.id)
        .await
        .expect("Poll must capture creation-time route and epoch");
    assert_eq!(capture.remote_epoch, remote.epoch);
    assert_eq!(
        ledger::invocation_snapshot(store.db(), &poll.id)
            .await
            .unwrap()
            .unwrap()
            .route
            .unwrap()
            .credential_revision,
        1
    );
    let target = pin_lease_target::Entity::find()
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(target.state, "pinned");
    assert!(
        ledger::lease_status(store.db(), &target.lease_id)
            .await
            .unwrap()
            .unwrap()
            .all_targets_pinned
    );
    let requests = server.received_requests().await.unwrap();
    let posts = requests
        .iter()
        .filter(|r| r.method.as_str() == "POST")
        .count();
    let gets = requests
        .iter()
        .filter(|r| r.method.as_str() == "GET")
        .count();
    assert_eq!(posts, 1);
    assert_eq!(gets, 1);
}

#[tokio::test]
async fn prelocked_reactivation_keeps_capture_and_rejects_changed_epoch_or_missing_history() {
    let server = MockServer::start().await;
    let cid = "bafy-prelocked-reactivation";
    let (store, _, provider) = published_store(&server.uri(), cid).await;
    store.db().execute_unprepared(&format!(
        "UPDATE remote_pins SET status='queued', request_id='request-1' WHERE provider='{provider}' AND cid='{cid}'"
    )).await.unwrap();
    let target = pin_lease_target::Entity::find()
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert!(project(&store, &target.id).await);
    let poll_job = poll(&store).await;
    let original = captured(&store, &poll_job.id).await.unwrap();

    let done = format!(
        "UPDATE pin_jobs SET state='done' WHERE id='{}'",
        poll_job.id
    );
    store.db().execute_unprepared(&done).await.unwrap();
    assert!(project(&store, &target.id).await);
    assert_eq!(poll(&store).await.state, "pending");
    assert_eq!(captured(&store, &poll_job.id).await.unwrap(), original);

    store.db().execute_unprepared(&format!(
        "UPDATE pin_jobs SET state='done' WHERE id='{}'; UPDATE remote_pins SET epoch=epoch+1 WHERE provider='{provider}' AND cid='{cid}'", poll_job.id
    )).await.unwrap();
    assert!(!project(&store, &target.id).await);
    assert_eq!(poll(&store).await.state, "done");
    assert_eq!(captured(&store, &poll_job.id).await.unwrap(), original);

    let reset_epoch =
        format!("UPDATE remote_pins SET epoch=epoch-1 WHERE provider='{provider}' AND cid='{cid}'");
    store.db().execute_unprepared(&reset_epoch).await.unwrap();
    let row = ledger::get(store.db(), &provider, cid)
        .await
        .unwrap()
        .unwrap();
    let original_route = row.route.clone();
    let mut changed: serde_json::Value =
        serde_json::from_str(original_route.as_deref().unwrap()).unwrap();
    changed["credential_revision"] = serde_json::json!(2);
    let mut update: remote_pin_ledger::ActiveModel = row.into();
    update.route = Set(Some(changed.to_string()));
    update.update(store.db()).await.unwrap();
    assert!(!project(&store, &target.id).await);
    assert_eq!(poll(&store).await.state, "done");
    assert_eq!(captured(&store, &poll_job.id).await.unwrap(), original);

    let row = ledger::get(store.db(), &provider, cid)
        .await
        .unwrap()
        .unwrap();
    let mut update: remote_pin_ledger::ActiveModel = row.into();
    update.route = Set(original_route);
    update.update(store.db()).await.unwrap();
    let forget = format!(
        "DELETE FROM pin_invocation_routes WHERE job_id='{}'",
        poll_job.id
    );
    store.db().execute_unprepared(&forget).await.unwrap();
    assert!(!project(&store, &target.id).await);
    assert_eq!(poll(&store).await.state, "done");
    assert!(captured(&store, &poll_job.id).await.is_none());
}
