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
        entities::{pin_invocation_route, pin_job, pin_resource_history, remote_pin},
        pinning::{
            jobs, ledger,
            publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
            quota,
        },
    },
};
use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait, Set, TransactionTrait};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const CID: &str = "bafy-stage2-submit-retry";

async fn setup(server: &MockServer) -> (Store, Arc<PinningCoordinator>, String) {
    let config: Config = toml::from_str(&format!(
        "[pinning_identity]\nprimary_storage_domain='local'\n[[pinning_identity.providers]]\nconfig_name='provider'\nprovider_id='stable'\ndisplay_name='Provider'\nbackend='filebase'\nscope='test-account'\nstorage_domain='remote'\ncredential_revision=1\nendpoint_revision=1\nsecret_ref='env:STAGE2_TOKEN'\napi_profile='filebase-psa'\nstrategy='cid'\ncleanup='managed'\n[[pinning.providers]]\nname='provider'\nkind='filebase'\ntoken_env='STAGE2_TOKEN'\nendpoint='{}'\npriority=1\nmax_bytes=1000\nmax_pins=10\n[pinning]\nworker_interval='1s'\n",
        server.uri()
    ))
    .unwrap();
    let runtime = PinningCoordinator::build(
        ValidatedPinningConfig::from_config(&config, |_| Some("test-token".into())).unwrap(),
    )
    .unwrap();
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "retry", None).await.unwrap();
    let store = Store::new(db);
    runtime.register_identities(&store).await.unwrap();
    let provider = runtime.provider_limits().keys().next().unwrap().clone();
    (store, runtime, provider)
}

async fn publish(store: &Store, runtime: &PinningCoordinator, provider: &str, key: &str) {
    publication::publish_object(
        store.db(),
        PublicationRequest {
            object: PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                "retry",
                key,
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
                    providers: vec![provider.into()],
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
}

async fn first_rejection(
    server: &MockServer,
    store: &Store,
    runtime: &Arc<PinningCoordinator>,
) -> (pin_job::Model, i64) {
    let worker = runtime.start(store.clone(), CancellationToken::new());
    let settled = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let job = pin_job::Entity::find()
                .one(store.db())
                .await
                .unwrap()
                .unwrap();
            if job.state == "pending"
                && job.submit_phase.as_deref() == Some("recovery_backoff")
                && jobs::submission_history(store.db(), &job.id)
                    .await
                    .unwrap()
                    .is_some_and(|history| history.effect == "not_created")
            {
                break job;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    let job = settled.expect("the first POST must be definitively rejected");
    assert_eq!(posts(server).await, 1);
    let captured = pin_invocation_route::Entity::find_by_id(job.id.clone())
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    (job, captured.remote_epoch)
}

async fn posts(server: &MockServer) -> usize {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.method.as_str() == "POST" && r.url.path() == "/pins")
        .count()
}

async fn mount_responses(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/pins"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "30"))
        .with_priority(1)
        .up_to_n_times(1)
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/pins"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "requestid": "created-on-retry", "status": "pinned", "pin": {"cid": CID}
        })))
        .with_priority(2)
        .mount(server)
        .await;
}

async fn make_due(store: &Store, job_id: &str) {
    let mut job: pin_job::ActiveModel = pin_job::Entity::find_by_id(job_id)
        .one(store.db())
        .await
        .unwrap()
        .unwrap()
        .into();
    job.next_attempt_at = Set(Utc::now() - chrono::Duration::seconds(1));
    job.update(store.db()).await.unwrap();
}

#[tokio::test]
async fn definite_rejection_then_shared_cid_retries_same_submit_after_reference_cas() {
    let server = MockServer::start().await;
    mount_responses(&server).await;
    let (store, runtime, provider) = setup(&server).await;
    publish(&store, &runtime, &provider, "first").await;
    let (job, original_epoch) = first_rejection(&server, &store, &runtime).await;

    publish(&store, &runtime, &provider, "second").await;
    let reused = remote_pin::Entity::find_by_id((provider.clone(), CID.to_owned()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert!(reused.epoch > original_epoch);
    assert_eq!(reused.status, "reserved");
    assert!(
        pin_resource_history::Entity::find()
            .one(store.db())
            .await
            .unwrap()
            .is_none()
    );
    make_due(&store, &job.id).await;
    let worker = runtime.start(store.clone(), CancellationToken::new());
    let result = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let remote = remote_pin::Entity::find_by_id((provider.clone(), CID.to_owned()))
                .one(store.db())
                .await
                .unwrap()
                .unwrap();
            if remote.request_id.as_deref() == Some("created-on-retry") {
                break remote;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    let remote = result.expect("same-lifetime Submit must retry after reference CAS");
    assert_eq!(posts(&server).await, 2);
    assert_eq!(remote.status, "pinned");
    assert_eq!(
        jobs::submission_history(store.db(), &job.id)
            .await
            .unwrap()
            .unwrap()
            .submit_calls,
        2
    );
    assert_eq!(
        pin_invocation_route::Entity::find_by_id(job.id)
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .remote_epoch,
        remote.epoch
    );
}

#[tokio::test]
async fn archived_release_blocks_original_submit_from_new_lifetime_even_on_same_route() {
    let server = MockServer::start().await;
    mount_responses(&server).await;
    let (store, runtime, provider) = setup(&server).await;
    publish(&store, &runtime, &provider, "first").await;
    let (job, original_epoch) = first_rejection(&server, &store, &runtime).await;

    // Force a confirmed release while the original target still exists so the
    // invocation fence, rather than a cancelled target, must reject this retry.
    let txn = store.db().begin().await.unwrap();
    quota::confirmed_release(&txn, &provider, CID, original_epoch, Utc::now())
        .await
        .unwrap();
    txn.commit().await.unwrap();
    assert!(
        pin_resource_history::Entity::find_by_id((
            provider.clone(),
            CID.to_owned(),
            original_epoch
        ))
        .one(store.db())
        .await
        .unwrap()
        .is_some()
    );
    publish(&store, &runtime, &provider, "second").await;
    let new_remote = remote_pin::Entity::find_by_id((provider.clone(), CID.to_owned()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert!(new_remote.epoch > original_epoch);
    assert_eq!(new_remote.status, "reserved");
    make_due(&store, &job.id).await;
    let claimed = jobs::claim_due_jobs(store.db(), Utc::now(), chrono::Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(claimed.model.id, job.id);
    let txn = store.db().begin().await.unwrap();
    assert_eq!(
        jobs::prepare_submit_call(&txn, &claimed, Utc::now())
            .await
            .unwrap(),
        jobs::SubmitCallDecision::ReadyToCall,
        "the original target and generation must still pass Stage 1's preflight"
    );
    assert!(
        jobs::record_submit_invocation(&txn, &claimed, "psa", "cid", Utc::now())
            .await
            .is_err(),
        "an archived release must fence the old invocation before HTTP"
    );
    txn.rollback().await.unwrap();
    assert_eq!(
        posts(&server).await,
        1,
        "original Submit must not POST in new resource lifetime"
    );
    assert_eq!(
        pin_invocation_route::Entity::find_by_id(job.id.clone())
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .remote_epoch,
        original_epoch
    );
    assert_eq!(
        jobs::submission_history(store.db(), &job.id)
            .await
            .unwrap()
            .unwrap()
            .submit_calls,
        1
    );
    assert_eq!(
        ledger::get(store.db(), &provider, CID)
            .await
            .unwrap()
            .unwrap()
            .effect,
        "reserved"
    );
}

#[tokio::test]
async fn unknown_prior_submit_effect_cannot_advance_across_reference_cas() {
    let server = MockServer::start().await;
    mount_responses(&server).await;
    let (store, runtime, provider) = setup(&server).await;
    publish(&store, &runtime, &provider, "first").await;
    let (job, original_epoch) = first_rejection(&server, &store, &runtime).await;
    publish(&store, &runtime, &provider, "second").await;
    assert!(
        remote_pin::Entity::find_by_id((provider.clone(), CID.to_owned()))
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .epoch
            > original_epoch
    );
    // Losing certainty about the first POST is not a not-created retry.
    store
        .db()
        .execute_unprepared(&format!(
            "UPDATE pin_submit_history SET effect='unknown' WHERE job_id='{}'",
            job.id
        ))
        .await
        .unwrap();
    make_due(&store, &job.id).await;
    let claimed = jobs::claim_due_jobs(store.db(), Utc::now(), chrono::Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(claimed.model.id, job.id);
    let txn = store.db().begin().await.unwrap();
    assert_eq!(
        jobs::prepare_submit_call(&txn, &claimed, Utc::now())
            .await
            .unwrap(),
        jobs::SubmitCallDecision::ReadyToCall
    );
    assert!(
        jobs::record_submit_invocation(&txn, &claimed, "psa", "cid", Utc::now())
            .await
            .is_err()
    );
    txn.rollback().await.unwrap();
    assert_eq!(posts(&server).await, 1);
    assert_eq!(
        pin_invocation_route::Entity::find_by_id(job.id)
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .remote_epoch,
        original_epoch
    );
}
