use chrono::Utc;
use ipfs_s3_gateway::pinning::provider::RemotePinStatus;
use ipfs_s3_gateway::pinning::{
    config::{LeaseDuration, ProviderMode},
    policy::{LeaseIntent, LeaseSource, PublicationPolicy},
    tags::ContentMode,
};
use ipfs_s3_gateway::{
    config::Config,
    pinning::{config::ValidatedPinningConfig, coordinator::PinningCoordinator},
    store::{
        self, Store,
        entities::{pin_invocation_route, remote_pin},
        pinning::{
            jobs, leases, ledger,
            publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
            quota,
        },
    },
};
use sea_orm::{ConnectionTrait, EntityTrait, TransactionTrait};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

fn config(endpoint: &str, revision: u64) -> Config {
    let text = format!(
        "[pinning_identity]\nprimary_storage_domain='local'\n[[pinning_identity.providers]]\nconfig_name='provider'\nprovider_id='stable'\ndisplay_name='Provider'\nbackend='filebase'\nscope='test-account'\nstorage_domain='remote'\ncredential_revision={revision}\nendpoint_revision=1\nsecret_ref='env:STAGE2_TOKEN'\napi_profile='filebase-psa'\nstrategy='cid'\ncleanup='managed'\n[[pinning.providers]]\nname='provider'\nkind='filebase'\ntoken_env='STAGE2_TOKEN'\nendpoint='{endpoint}'\npriority=1\nmax_bytes=1000\nmax_pins=10\n[pinning]\nworker_interval='1s'\n"
    );
    toml::from_str(&text).unwrap()
}

fn coordinator(cfg: &Config) -> Arc<PinningCoordinator> {
    PinningCoordinator::build(
        ValidatedPinningConfig::from_config(cfg, |_| Some("test-token".into())).unwrap(),
    )
    .unwrap()
}

async fn setup(server: &MockServer) -> (Store, Arc<PinningCoordinator>, String, String, String) {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "provenance", None)
        .await
        .unwrap();
    let store = Store::new(db);
    let runtime = coordinator(&config(&server.uri(), 1));
    runtime.register_identities(&store).await.unwrap();
    let provider = runtime.provider_limits().keys().next().unwrap().clone();
    let cid = "bafy-provenance".to_owned();
    publication::publish_object(
        store.db(),
        PublicationRequest {
            object: PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                "provenance",
                "key",
                cid.clone(),
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
                cid: cid.clone(),
                logical_size: 100,
            },
        },
        runtime.provider_limits(),
    )
    .await
    .unwrap();
    let target = ipfs_s3_gateway::store::entities::pin_lease_target::Entity::find()
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    (store, runtime, provider, cid, target.id)
}

#[tokio::test]
async fn psa_existing_resource_never_gains_managed_cleanup_from_submit() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/pins")).respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"requestid":"preexisting","status":"pinned","pin":{"cid":"bafy-provenance"}}))).expect(1).mount(&server).await;
    let (store, runtime, provider, cid, target_id) = setup(&server).await;
    let worker = runtime.start(store.clone(), CancellationToken::new());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if ledger::status(store.db(), &provider, &cid)
                .await
                .unwrap()
                .is_some_and(|status| status.remote_status == "pinned")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    worker.shutdown(Duration::from_secs(5)).await;
    let row = ledger::get(store.db(), &provider, &cid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.ownership, "unknown");
    assert_eq!(
        ledger::status(store.db(), &provider, &cid)
            .await
            .unwrap()
            .unwrap()
            .remote_ref
            .unwrap()
            .resource_type,
        ipfs_s3_gateway::pinning::identity::RemoteResourceType::PsaRequest
    );
    assert!(
        !ledger::cleanup_allowed(store.db(), &provider, &cid)
            .await
            .unwrap()
    );
    let target = ipfs_s3_gateway::store::entities::pin_lease_target::Entity::find_by_id(target_id)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let txn = store.db().begin().await.unwrap();
    leases::cancel_lease(&txn, &target.lease_id, Utc::now())
        .await
        .unwrap();
    txn.commit().await.unwrap();
    let worker = runtime.start(store.clone(), CancellationToken::new());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if ledger::get(store.db(), &provider, &cid)
                .await
                .unwrap()
                .unwrap()
                .effect
                == "retained"
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    worker.shutdown(Duration::from_secs(5)).await;
    assert_eq!(
        quota::read_usage(store.db(), &provider)
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        1
    );
    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method.as_str() == "POST")
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method.as_str() == "DELETE")
            .count(),
        0
    );
}

#[tokio::test]
async fn poll_and_unpin_capture_the_original_resource_epoch_and_route() {
    let server = MockServer::start().await;
    let (store, _, provider, cid, target_id) = setup(&server).await;
    let target = ipfs_s3_gateway::store::entities::pin_lease_target::Entity::find_by_id(target_id)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let lease =
        ipfs_s3_gateway::store::entities::pin_lease::Entity::find_by_id(target.lease_id.clone())
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
    let remote = remote_pin::Entity::find_by_id((provider.clone(), cid.clone()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    store.db().execute_unprepared(&format!("UPDATE remote_pins SET status='queued', request_id='old-request' WHERE provider='{provider}' AND cid='{cid}'")).await.unwrap();
    let jobs_to_check = [
        jobs::poll_job(
            &provider,
            &cid,
            &target.lease_id,
            &target.id,
            lease.generation,
            "old-request",
            Utc::now(),
        ),
        jobs::unpin_job(&provider, &cid, remote.epoch, Utc::now()),
        jobs::reconcile_job(&provider, &cid, remote.epoch, Utc::now()),
    ];
    let mut ids = Vec::new();
    for job in jobs_to_check {
        let id = match &job {
            jobs::NewPinJob::Target(job) => job.id.clone(),
            jobs::NewPinJob::Remote(job) => job.id.clone(),
        };
        jobs::enqueue_job(store.db(), job).await.unwrap();
        ids.push(id);
    }
    store.db().execute_unprepared(&format!("UPDATE remote_pins SET status='absent', request_id=NULL, epoch=epoch+1 WHERE provider='{provider}' AND cid='{cid}'")).await.unwrap();
    ledger::mark_effect(store.db(), &provider, &cid, "absent")
        .await
        .unwrap();
    let next = coordinator(&config(&server.uri(), 2));
    next.register_identities(&store).await.unwrap();
    ledger::capture_reallocation(store.db(), &provider, &cid)
        .await
        .unwrap();
    for id in &ids {
        let route = pin_invocation_route::Entity::find_by_id(id.clone())
            .one(store.db())
            .await
            .unwrap()
            .expect("route captured before reallocation");
        assert_eq!(route.remote_epoch, remote.epoch, "{id}");
        assert_eq!(
            ledger::invocation_snapshot(store.db(), id)
                .await
                .unwrap()
                .unwrap()
                .route
                .unwrap()
                .credential_revision,
            1,
            "{id}"
        );
    }
    store
        .db()
        .execute_unprepared("UPDATE pin_jobs SET state='done' WHERE operation='submit'")
        .await
        .unwrap();
    let worker = next.start(store.clone(), CancellationToken::new());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let mut parked = true;
            for id in &ids {
                let job = ipfs_s3_gateway::store::entities::pin_job::Entity::find_by_id(id.clone())
                    .one(store.db())
                    .await
                    .unwrap()
                    .unwrap();
                parked &= job.last_error.as_deref()
                    == Some("historical identity unavailable; needs_attention");
            }
            if parked {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    worker.shutdown(Duration::from_secs(5)).await;
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn queued_and_failed_requests_do_not_confirm_a_pin_or_claim_availability() {
    let server = MockServer::start().await;
    let (store, _, provider, cid, target_id) = setup(&server).await;
    let target = ipfs_s3_gateway::store::entities::pin_lease_target::Entity::find_by_id(target_id)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    for status in [RemotePinStatus::Queued, RemotePinStatus::Failed] {
        ledger::observe(
            store.db(),
            &provider,
            &cid,
            status,
            ledger::Ownership::Unknown,
            Utc::now(),
        )
        .await
        .unwrap();
        let row = ledger::get(store.db(), &provider, &cid)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(row.effect, "confirmed");
        assert_eq!(row.ownership, "unknown");
    }
    // A projection left pinned cannot prove availability without the remote
    // and ledger jointly confirming this lifetime.
    store
        .db()
        .execute_unprepared(&format!(
            "UPDATE pin_lease_targets SET state='pinned' WHERE id='{}'",
            target.id
        ))
        .await
        .unwrap();
    let status = ledger::lease_status(store.db(), &target.lease_id)
        .await
        .unwrap()
        .unwrap();
    assert!(!status.any_provider_available);
    assert!(!status.all_targets_pinned);
    assert_eq!(status.pinned_targets, 0);
    ledger::observe(
        store.db(),
        &provider,
        &cid,
        RemotePinStatus::Pinned,
        ledger::Ownership::Unknown,
        Utc::now(),
    )
    .await
    .unwrap();
    ledger::observe(
        store.db(),
        &provider,
        &cid,
        RemotePinStatus::Failed,
        ledger::Ownership::Unknown,
        Utc::now(),
    )
    .await
    .unwrap();
    assert_eq!(
        ledger::get(store.db(), &provider, &cid)
            .await
            .unwrap()
            .unwrap()
            .effect,
        "confirmed",
        "a failure does not erase historical pin evidence"
    );
}

#[tokio::test]
async fn submit_queued_and_failed_responses_do_not_confirm_ledger_effect() {
    for (raw, projected) in [("queued", "queued"), ("failed", "failed")] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/pins"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "requestid":"possibly-existing", "status":raw,
                "pin":{"cid":"bafy-provenance"}
            })))
            .mount(&server)
            .await;
        let (store, runtime, provider, cid, _) = setup(&server).await;
        let worker = runtime.start(store.clone(), CancellationToken::new());
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let remote = remote_pin::Entity::find_by_id((provider.clone(), cid.clone()))
                    .one(store.db())
                    .await
                    .unwrap()
                    .unwrap();
                if remote.status == projected && remote.request_id.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        worker.shutdown(Duration::from_secs(5)).await;
        let row = ledger::get(store.db(), &provider, &cid)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(row.effect, "confirmed", "{raw}");
        assert_eq!(row.ownership, "unknown", "{raw}");
    }
}

#[tokio::test]
async fn legacy_poll_and_unpin_without_captured_route_are_parked_without_network_io() {
    let server = MockServer::start().await;
    let (store, runtime, provider, cid, target_id) = setup(&server).await;
    let target = ipfs_s3_gateway::store::entities::pin_lease_target::Entity::find_by_id(target_id)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let lease =
        ipfs_s3_gateway::store::entities::pin_lease::Entity::find_by_id(target.lease_id.clone())
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
    let remote = remote_pin::Entity::find_by_id((provider.clone(), cid.clone()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    store.db().execute_unprepared(&format!("UPDATE pin_jobs SET state='done' WHERE operation='submit'; UPDATE remote_pins SET request_id='old-request', status='pinned' WHERE provider='{provider}' AND cid='{cid}'")).await.unwrap();
    let id = format!("unpin:{provider}:{cid}:e{}", remote.epoch);
    let poll_id = match jobs::poll_job(
        &provider,
        &cid,
        &lease.id,
        &target.id,
        lease.generation,
        "old-request",
        Utc::now(),
    ) {
        jobs::NewPinJob::Target(job) => job.id,
        jobs::NewPinJob::Remote(_) => unreachable!(),
    };
    store.db().execute_unprepared(&format!("INSERT INTO pin_jobs (id,operation,provider,cid,expected_remote_epoch,state,next_attempt_at,submit_phase) VALUES ('{id}','unpin','{provider}','{cid}',{},'pending','2026-09-20T00:00:00Z',NULL)", remote.epoch)).await.unwrap();
    store.db().execute_unprepared(&format!("INSERT INTO pin_jobs (id,operation,provider,cid,lease_id,target_id,expected_generation,state,next_attempt_at,submit_phase) VALUES ('{poll_id}','poll','{provider}','{cid}','{}','{}',{},'pending','2026-09-20T00:00:00Z',NULL)", lease.id, target.id, lease.generation)).await.unwrap();
    for job_id in [&id, &poll_id] {
        assert!(
            ledger::invocation_snapshot(store.db(), job_id)
                .await
                .unwrap()
                .unwrap()
                .route
                .is_none()
        );
    }
    let worker = runtime.start(store.clone(), CancellationToken::new());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let mut parked = true;
            for job_id in [&id, &poll_id] {
                let job =
                    ipfs_s3_gateway::store::entities::pin_job::Entity::find_by_id(job_id.clone())
                        .one(store.db())
                        .await
                        .unwrap()
                        .unwrap();
                parked &= job.last_error.as_deref()
                    == Some("historical identity unavailable; needs_attention");
            }
            if parked {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    worker.shutdown(Duration::from_secs(5)).await;
    assert!(server.received_requests().await.unwrap().is_empty());
    assert!(
        !ledger::cleanup_allowed(store.db(), &provider, &cid)
            .await
            .unwrap()
    );
}
