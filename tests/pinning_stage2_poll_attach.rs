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
            pin_invocation_route, pin_job, pin_lease_target, pin_provider_usage, remote_pin,
        },
        pinning::{
            jobs, leases, ledger,
            publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
        },
    },
};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const CID: &str = "bafy-poll-shared-attach";
const REQUEST: &str = "same-lifetime-request";

fn coordinator(endpoint: &str, credential_revision: u64) -> Arc<PinningCoordinator> {
    let config: Config = toml::from_str(&format!(
        "[pinning_identity]\nprimary_storage_domain='local'\n[[pinning_identity.providers]]\nconfig_name='provider'\nprovider_id='stable'\ndisplay_name='Provider'\nbackend='filebase'\nscope='test-account'\nstorage_domain='remote'\ncredential_revision={credential_revision}\nendpoint_revision=1\nsecret_ref='env:STAGE2_TOKEN'\napi_profile='filebase-psa'\nstrategy='cid'\ncleanup='managed'\n[[pinning.providers]]\nname='provider'\nkind='filebase'\ntoken_env='STAGE2_TOKEN'\nendpoint='{endpoint}'\npriority=1\nmax_bytes=1000\nmax_pins=10\n[pinning]\nworker_interval='1s'\n"
    ))
    .unwrap();
    PinningCoordinator::build(
        ValidatedPinningConfig::from_config(&config, |_| Some("test-token".into())).unwrap(),
    )
    .unwrap()
}

async fn publish(store: &Store, runtime: &PinningCoordinator, key: &str) {
    let provider = runtime.provider_limits().keys().next().unwrap().clone();
    publication::publish_object(
        store.db(),
        PublicationRequest {
            object: PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                "poll-attach",
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
                    policy_id: "shared".into(),
                    provider_mode: ProviderMode::All,
                    providers: vec![provider],
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

async fn poll(store: &Store) -> Option<pin_job::Model> {
    pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("poll"))
        .one(store.db())
        .await
        .unwrap()
}

async fn queued_fixture(
    server: &MockServer,
) -> (tempfile::TempDir, Store, Arc<PinningCoordinator>, String) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("poll-attach.sqlite");
    let url = format!(
        "sqlite://{}?mode=rwc",
        path.display().to_string().replace('\\', "/")
    );
    let db = store::connect_database(&url).await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "poll-attach", None)
        .await
        .unwrap();
    let store = Store::new(db);
    let runtime = coordinator(&server.uri(), 1);
    runtime.register_identities(&store).await.unwrap();
    let provider = runtime.provider_limits().keys().next().unwrap().clone();
    publish(&store, &runtime, "first").await;
    let worker = runtime.start(store.clone(), CancellationToken::new());
    tokio::time::timeout(Duration::from_secs(4), async {
        while poll(&store).await.is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("POST queued must create a captured Poll");
    worker.shutdown(Duration::from_secs(4)).await;
    assert_eq!(poll(&store).await.unwrap().state, "pending");
    (directory, store, runtime, provider)
}

#[tokio::test]
async fn queued_poll_survives_shared_attach_without_second_post_or_rebinding() {
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

    let (_directory, store, runtime, provider) = queued_fixture(&server).await;
    let original_poll = poll(&store).await.unwrap();
    assert_eq!(original_poll.state, "pending");
    let original_capture = pin_invocation_route::Entity::find_by_id(&original_poll.id)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let before = remote_pin::Entity::find_by_id((provider.clone(), CID.to_owned()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.status, "queued");
    assert_eq!(before.request_id.as_deref(), Some(REQUEST));
    assert_eq!(original_capture.remote_epoch, before.epoch);

    publish(&store, &runtime, "second").await;
    let after_attach = remote_pin::Entity::find_by_id((provider.clone(), CID.to_owned()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_attach.epoch, before.epoch + 1);
    assert_eq!(after_attach.request_id, before.request_id);
    assert_eq!(poll(&store).await.unwrap().id, original_poll.id);
    assert!(
        jobs::check_target_job_generation(store.db(), &original_poll)
            .await
            .unwrap()
    );
    let snapshot = leases::remote_work_snapshot(store.db(), &provider, CID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        snapshot.desired.first().unwrap().target_id,
        original_poll.target_id.as_deref().unwrap()
    );
    assert_eq!(
        pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .all(store.db())
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        pin_provider_usage::Entity::find_by_id(&provider)
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        1
    );

    let worker = runtime.start(store.clone(), CancellationToken::new());
    let converged = tokio::time::timeout(Duration::from_secs(9), async {
        loop {
            if remote_pin::Entity::find_by_id((provider.clone(), CID.to_owned()))
                .one(store.db())
                .await
                .unwrap()
                .is_some_and(|remote| remote.status == "pinned")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    worker.shutdown(Duration::from_secs(4)).await;
    let current_poll = poll(&store).await.unwrap();
    let remote = remote_pin::Entity::find_by_id((provider.clone(), CID.to_owned()))
        .one(store.db())
        .await
        .unwrap();
    let received_count = server.received_requests().await.unwrap().len();
    assert!(
        converged.is_ok(),
        "shared attach did not complete Poll: poll={current_poll:?}, remote={remote:?}, request_count={received_count}"
    );
    assert_eq!(current_poll.id, original_poll.id);
    assert_eq!(current_poll.state, "done");
    assert_eq!(
        pin_invocation_route::Entity::find_by_id(&current_poll.id)
            .one(store.db())
            .await
            .unwrap()
            .unwrap(),
        original_capture,
        "Poll retains its historical route and original epoch"
    );
    let targets = pin_lease_target::Entity::find()
        .all(store.db())
        .await
        .unwrap();
    assert_eq!(targets.len(), 2);
    assert!(targets.iter().all(|target| target.state == "pinned"));
    assert!(targets.iter().all(|target| target.provider == provider));
    for target in &targets {
        assert!(
            ledger::lease_status(store.db(), &target.lease_id)
                .await
                .unwrap()
                .unwrap()
                .all_targets_pinned
        );
    }
    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.method.as_str() == "POST")
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.method.as_str() == "GET")
            .count(),
        1
    );
}

#[tokio::test]
async fn archived_release_or_route_revision_never_rebinds_old_poll() {
    for archived_release in [true, false] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/pins"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "requestid": REQUEST, "status": "queued", "pin": {"cid": CID}
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (_directory, store, runtime, provider) = queued_fixture(&server).await;
        let old_poll = poll(&store).await.unwrap();
        let old_remote = remote_pin::Entity::find_by_id((provider.clone(), CID.to_owned()))
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
        publish(&store, &runtime, "second").await;
        let active_runtime = if archived_release {
            // A confirmed-absent archive is a lifetime boundary even if a later
            // allocation happens to report the same opaque provider request ID.
            ledger::archive_confirmed_release(store.db(), &old_remote)
                .await
                .unwrap();
            runtime
        } else {
            let revised = coordinator(&server.uri(), 2);
            revised.register_identities(&store).await.unwrap();
            revised
        };
        let worker = active_runtime.start(store.clone(), CancellationToken::new());
        tokio::time::timeout(Duration::from_secs(9), async {
            loop {
                let row = poll(&store).await.unwrap();
                if row.state == "running" && row.locked_until.is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("historically invalid Poll must park");
        worker.shutdown(Duration::from_secs(4)).await;
        let parked = poll(&store).await.unwrap();
        assert_eq!(parked.id, old_poll.id);
        assert_eq!(
            parked.last_error.as_deref(),
            Some("historical identity unavailable; needs_attention")
        );
        let received = server.received_requests().await.unwrap();
        assert_eq!(
            received
                .iter()
                .filter(|request| request.method.as_str() == "POST")
                .count(),
            1
        );
        assert_eq!(
            received
                .iter()
                .filter(|request| request.method.as_str() == "GET")
                .count(),
            0
        );
    }
}
