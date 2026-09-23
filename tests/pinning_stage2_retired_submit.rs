use std::{
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};

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
        pinning::publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
    },
};
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Set};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const CID: &str = "bafy-stage2-inflight-retirement";

fn runtime(endpoint: &str, retired: bool, credential_revision: u64) -> Arc<PinningCoordinator> {
    let config: Config = toml::from_str(&format!(
        "[pinning_identity]\nprimary_storage_domain='local'\n[[pinning_identity.providers]]\nconfig_name='provider'\nprovider_id='stable'\ndisplay_name='Provider'\nbackend='filebase'\nscope='test-account'\nstorage_domain='remote'\ncredential_revision={credential_revision}\nendpoint_revision=1\nsecret_ref='env:STAGE2_TOKEN'\napi_profile='filebase-psa'\nstrategy='cid'\nretired={retired}\ncleanup='managed'\n[[pinning.providers]]\nname='provider'\nkind='filebase'\ntoken_env='STAGE2_TOKEN'\nendpoint='{endpoint}'\npriority=1\nmax_bytes=1000\nmax_pins=10\n[pinning]\nworker_interval='1s'\n"
    )).unwrap();
    PinningCoordinator::build(
        ValidatedPinningConfig::from_config(&config, |_| Some("test-token".into())).unwrap(),
    )
    .unwrap()
}

fn pinata_runtime(endpoint: &str, strategy: &str) -> Arc<PinningCoordinator> {
    let config: Config = toml::from_str(&format!(
        "[pinning_identity]\nprimary_storage_domain='local'\n[[pinning_identity.providers]]\nconfig_name='provider'\nprovider_id='stable'\ndisplay_name='Provider'\nbackend='pinata'\nscope='test-account'\nstorage_domain='remote'\ncredential_revision=1\nendpoint_revision=1\nsecret_ref='env:STAGE2_TOKEN'\napi_profile='pinata-v3'\nstrategy='{strategy}'\ncleanup='managed'\n[[pinning.providers]]\nname='provider'\nkind='pinata'\napi='v3'\nstrategy='{strategy}'\ntoken_env='STAGE2_TOKEN'\nendpoint='{endpoint}/v3'\npriority=1\nmax_bytes=1000\nmax_pins=10\n[pinning]\nworker_interval='1s'\n"
    )).unwrap();
    PinningCoordinator::build(
        ValidatedPinningConfig::from_config(&config, |_| Some("test-token".into())).unwrap(),
    )
    .unwrap()
}

async fn publish(store: &Store, runtime: &PinningCoordinator, key: &str) -> bool {
    let provider = runtime.provider_limits().keys().next().unwrap().clone();
    publication::publish_object(
        store.db(),
        PublicationRequest {
            object: PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                "inflight",
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
    .is_ok()
}

#[derive(Clone, Copy)]
enum Change {
    Retire,
    RetireAfterAttach,
    Strategy,
    PinataStrategy,
    Credential,
    CredentialAfterPoll,
}

async fn inflight_submit(change: Change) {
    let server = MockServer::start().await;
    let pinata = matches!(change, Change::PinataStrategy);
    let (arrived, posted) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let released = Arc::new(Mutex::new(released));
    Mock::given(method("POST"))
        .and(path(if pinata { "/v3/files/public/pin_by_cid" } else { "/pins" }))
        .respond_with(move |_: &wiremock::Request| {
            arrived.send(()).unwrap();
            released.lock().unwrap().recv_timeout(Duration::from_secs(8)).unwrap();
            ResponseTemplate::new(200).set_body_json(if pinata {
                serde_json::json!({"data": {"id": "inflight-request", "status": "prechecking", "cid": CID}})
            } else {
                serde_json::json!({"requestid": "inflight-request", "status": "queued", "pin": {"cid": CID}})
            })
        })
        .expect(1)
        .mount(&server).await;
    Mock::given(method("GET"))
        .and(path(if pinata { "/v3/files/public" } else { "/pins/inflight-request" }))
        .respond_with(ResponseTemplate::new(200).set_body_json(if pinata {
            serde_json::json!({"data": {"files": [{"id": "inflight-request", "cid": CID}]}})
        } else {
            serde_json::json!({"requestid": "inflight-request", "status": "pinned", "pin": {"cid": CID}})
        }))
        .mount(&server).await;
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "inflight", None).await.unwrap();
    let store = Store::new(db);
    let original = if pinata {
        pinata_runtime(&server.uri(), "cid")
    } else {
        runtime(&server.uri(), false, 1)
    };
    original.register_identities(&store).await.unwrap();
    let provider = original.provider_limits().keys().next().unwrap().clone();
    assert!(publish(&store, &original, "original").await);
    let submit = pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("submit"))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let captured = pin_invocation_route::Entity::find_by_id(&submit.id)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let worker = original.start(store.clone(), CancellationToken::new());
    tokio::time::timeout(
        Duration::from_secs(8),
        tokio::task::spawn_blocking(move || {
            posted.recv_timeout(Duration::from_secs(7)).unwrap();
        }),
    )
    .await
    .unwrap()
    .unwrap();

    let attached = matches!(change, Change::RetireAfterAttach);
    if attached {
        assert!(publish(&store, &original, "attached-before-retirement").await);
        let remote = remote_pin::Entity::find_by_id((provider.clone(), CID.to_owned()))
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
        assert!(remote.epoch > captured.remote_epoch);
    }
    let runner = match change {
        Change::Retire | Change::RetireAfterAttach => {
            let new_runtime = runtime(&server.uri(), true, 1);
            new_runtime.register_identities(&store).await.unwrap();
            new_runtime
        }
        Change::PinataStrategy => {
            let new_runtime = pinata_runtime(&server.uri(), "upload");
            new_runtime.register_identities(&store).await.unwrap();
            new_runtime
        }
        Change::CredentialAfterPoll => original.clone(),
        Change::Strategy | Change::Credential => {
            let row = pin_provider_route::Entity::find_by_id(&provider)
                .one(store.db())
                .await
                .unwrap()
                .unwrap();
            let mut snapshot: serde_json::Value = serde_json::from_str(&row.snapshot).unwrap();
            match change {
                Change::Strategy => snapshot["strategy"] = serde_json::json!("upload"),
                Change::Credential => snapshot["credential_revision"] = serde_json::json!(2),
                Change::Retire
                | Change::RetireAfterAttach
                | Change::PinataStrategy
                | Change::CredentialAfterPoll => {
                    unreachable!()
                }
            }
            let mut changed: pin_provider_route::ActiveModel = row.into();
            changed.snapshot = Set(snapshot.to_string());
            changed.update(store.db()).await.unwrap();
            original.clone()
        }
    };
    if !matches!(change, Change::CredentialAfterPoll) {
        assert!(
            !publish(&store, &runner, "new").await,
            "new publication must be denied"
        );
    }
    release.send(()).unwrap();
    let observed = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let submit = pin_job::Entity::find_by_id(&submit.id)
                .one(store.db())
                .await
                .unwrap()
                .unwrap();
            if submit.state == "done" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    worker.shutdown(Duration::from_secs(3)).await;
    assert!(observed.is_ok(), "Submit response should be durable");
    let remote = remote_pin::Entity::find_by_id((provider.clone(), CID.to_owned()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remote.status, if pinata { "pinning" } else { "queued" });
    let poll = pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("poll"))
        .one(store.db())
        .await
        .unwrap();
    if matches!(change, Change::Credential) {
        assert!(
            poll.is_none(),
            "credential revision cannot authorize historical GET"
        );
        return;
    }
    let poll = poll.expect("in-flight historical POST must schedule its read-only Poll");
    let poll_capture = pin_invocation_route::Entity::find_by_id(&poll.id)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(poll_capture.route, captured.route);
    assert_eq!(poll_capture.remote_epoch, remote.epoch);
    assert_eq!(poll.state, "pending");
    if matches!(change, Change::CredentialAfterPoll) {
        runtime(&server.uri(), false, 2)
            .register_identities(&store)
            .await
            .unwrap();
        assert!(!publish(&store, &runner, "new").await);
        let worker = runner.start(store.clone(), CancellationToken::new());
        let parked = tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let job = pin_job::Entity::find_by_id(&poll.id)
                    .one(store.db())
                    .await
                    .unwrap()
                    .unwrap();
                if job.state == "running"
                    && job.locked_until.is_none()
                    && job.last_error.as_deref()
                        == Some("historical identity unavailable; needs_attention")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        worker.shutdown(Duration::from_secs(3)).await;
        assert!(
            parked.is_ok(),
            "old Poll must be parked after credential rotation"
        );
        assert_eq!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .filter(|r| r.method.as_str() == "GET")
                .count(),
            0
        );
        return;
    }
    let worker = runner.start(store.clone(), CancellationToken::new());
    let converged = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if remote_pin::Entity::find_by_id((provider.clone(), CID.to_owned()))
                .one(store.db())
                .await
                .unwrap()
                .unwrap()
                .status
                == "pinned"
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    worker.shutdown(Duration::from_secs(3)).await;
    assert!(
        converged.is_ok(),
        "historical GET must converge on the original route"
    );
    assert_eq!(
        pin_lease_target::Entity::find()
            .all(store.db())
            .await
            .unwrap()
            .len(),
        if attached { 2 } else { 1 }
    );
    assert!(
        pin_lease_target::Entity::find()
            .all(store.db())
            .await
            .unwrap()
            .iter()
            .all(|target| target.state == "pinned")
    );
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
    assert_eq!(
        pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("reconcile"))
            .all(store.db())
            .await
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("unpin"))
            .all(store.db())
            .await
            .unwrap()
            .len(),
        0,
        "historical GET must not authorize cleanup"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retired_during_post_still_polls_the_captured_request() {
    inflight_submit(Change::Retire).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_attach_before_retirement_keeps_submit_lifetime_and_one_poll() {
    inflight_submit(Change::RetireAfterAttach).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changed_strategy_during_post_still_polls_the_captured_request() {
    inflight_submit(Change::Strategy).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_pinata_upload_runtime_polls_historical_cid_request() {
    inflight_submit(Change::PinataStrategy).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changed_credential_during_post_does_not_schedule_a_poll() {
    inflight_submit(Change::Credential).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changed_credential_after_poll_capture_blocks_historical_get() {
    inflight_submit(Change::CredentialAfterPoll).await;
}
