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
            pin_invocation_route, pin_job, pin_lease, pin_lease_target, pin_provider_route,
            pin_provider_usage, remote_pin,
        },
        pinning::{
            jobs, leases, ledger,
            publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
        },
    },
};
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Set, TransactionTrait};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const CID: &str = "bafy-stage2-poll-handoff";
const REQUEST: &str = "original-shared-request";

fn runtime(endpoint: &str, retired: bool, credential_revision: u64) -> Arc<PinningCoordinator> {
    let config: Config = toml::from_str(&format!(
        "[pinning_identity]\nprimary_storage_domain='local'\n[[pinning_identity.providers]]\nconfig_name='provider'\nprovider_id='stable'\ndisplay_name='Provider'\nbackend='filebase'\nscope='test-account'\nstorage_domain='remote'\ncredential_revision={credential_revision}\nendpoint_revision=1\nsecret_ref='env:STAGE2_TOKEN'\napi_profile='filebase-psa'\nstrategy='cid'\nretired={retired}\ncleanup='managed'\n[[pinning.providers]]\nname='provider'\nkind='filebase'\ntoken_env='STAGE2_TOKEN'\nendpoint='{endpoint}'\npriority=1\nmax_bytes=100\nmax_pins=1\n[pinning]\nworker_interval='1s'\n"
    )).unwrap();
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
                "handoff",
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

async fn job(store: &Store, id: &str) -> pin_job::Model {
    pin_job::Entity::find_by_id(id)
        .one(store.db())
        .await
        .unwrap()
        .unwrap()
}

async fn wait_done(store: &Store, id: &str) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while job(store, id).await.state != "done" {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("job must finish");
}

async fn cancel(store: &Store, lease_id: &str) {
    let txn = store.db().begin().await.unwrap();
    assert_eq!(
        leases::cancel_lease(&txn, lease_id, Utc::now())
            .await
            .unwrap(),
        leases::GenerationDecision::Current
    );
    txn.commit().await.unwrap();
}

async fn remote(store: &Store, provider: &str) -> remote_pin::Model {
    remote_pin::Entity::find_by_id((provider.to_owned(), CID.to_owned()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap()
}

async fn queued_pair(
    store: &Store,
    server: &MockServer,
) -> (
    Arc<PinningCoordinator>,
    pin_job::Model,
    pin_lease_target::Model,
) {
    // Hold the actual POST until B shares A's allocation. No clock-based race
    // decides which target is the original Submit/Poll owner.
    let (arrived, posted) = tokio::sync::oneshot::channel();
    let arrived = Mutex::new(Some(arrived));
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    Mock::given(method("POST"))
        .and(path("/pins"))
        .respond_with(move |_: &wiremock::Request| {
            arrived.lock().unwrap().take().unwrap().send(()).unwrap();
            released
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(8))
                .unwrap();
            ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"requestid": REQUEST, "status": "queued", "pin": {"cid": CID}}),
            )
        })
        .expect(1)
        .mount(server)
        .await;
    store::bucket::create(store.db(), "handoff", None)
        .await
        .unwrap();
    let original = runtime(&server.uri(), false, 1);
    original.register_identities(store).await.unwrap();
    publish(store, &original, "A").await;
    let submit = pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("submit"))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let worker = original.start(store.clone(), CancellationToken::new());
    tokio::time::timeout(Duration::from_secs(8), posted)
        .await
        .unwrap()
        .unwrap();
    publish(store, &original, "B").await;
    release.send(()).unwrap();
    wait_done(store, &submit.id).await;
    worker.shutdown(Duration::from_secs(3)).await;
    let poll = pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("poll"))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(poll.target_id, submit.target_id);
    assert_eq!(poll.state, "pending");
    let b = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::Id.ne(poll.target_id.as_ref().unwrap()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    (original, poll, b)
}

async fn handoff(cancel_first: bool, strategy_change: bool) {
    let server = MockServer::start().await;
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let store = Store::new(db);
    let (_original, a_poll, b) = queued_pair(&store, &server).await;
    let provider = &a_poll.provider;
    let a_capture = pin_invocation_route::Entity::find_by_id(&a_poll.id)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let before = remote(&store, provider).await;
    if cancel_first {
        cancel(&store, a_poll.lease_id.as_ref().unwrap()).await;
    }
    let retired = runtime(&server.uri(), !strategy_change, 1);
    retired.register_identities(&store).await.unwrap();
    if strategy_change {
        change_registered_route(&store, provider, "strategy", serde_json::json!("upload")).await;
    }
    if !cancel_first {
        cancel(&store, a_poll.lease_id.as_ref().unwrap()).await;
    }
    let after = remote(&store, provider).await;
    assert_eq!(after.epoch, before.epoch + 1);
    assert_eq!(after.request_id.as_deref(), Some(REQUEST));
    let reconcile = pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("reconcile"))
        .filter(pin_job::Column::ExpectedRemoteEpoch.eq(after.epoch))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let (arrived, observed) = tokio::sync::oneshot::channel();
    let arrived = Mutex::new(Some(arrived));
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    Mock::given(method("GET"))
        .and(path(format!("/pins/{REQUEST}")))
        .respond_with(move |_: &wiremock::Request| {
            let first = arrived.lock().unwrap().take();
            let status = if let Some(arrived) = first {
                arrived.send(()).unwrap();
                released
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(8))
                    .unwrap();
                "pinning"
            } else {
                "pinned"
            };
            ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"requestid": REQUEST, "status": status, "pin": {"cid": CID}}),
            )
        })
        .expect(2)
        .mount(&server)
        .await;
    let worker = retired.start(store.clone(), CancellationToken::new());
    wait_done(&store, &reconcile.id).await;
    let b_poll = pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("poll"))
        .filter(pin_job::Column::TargetId.eq(&b.id))
        .one(store.db())
        .await
        .unwrap();
    if b_poll.is_none() {
        worker.shutdown(Duration::from_secs(3)).await;
        panic!(
            "Reconcile finished but lost B's read-only Poll: A={:?}, remote={:?}, B={:?}",
            job(&store, &a_poll.id).await,
            remote(&store, provider).await,
            pin_lease_target::Entity::find_by_id(&b.id)
                .one(store.db())
                .await
                .unwrap()
        );
    }
    let b_poll = b_poll.unwrap();
    let b_capture = pin_invocation_route::Entity::find_by_id(&b_poll.id)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(b_capture.route, a_capture.route);
    assert_eq!(b_capture.remote_epoch, after.epoch);
    tokio::time::timeout(Duration::from_secs(8), observed)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        pin_lease::Entity::find_by_id(a_poll.lease_id.as_ref().unwrap())
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "cancelled"
    );
    assert!(
        !jobs::check_target_job_generation(store.db(), &a_poll)
            .await
            .unwrap()
    );
    release.send(()).unwrap();
    wait_done(&store, &b_poll.id).await;
    worker.shutdown(Duration::from_secs(3)).await;
    assert_eq!(
        pin_lease_target::Entity::find_by_id(&b.id)
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "pinned"
    );
    assert_eq!(remote(&store, provider).await.epoch, after.epoch);
    assert_eq!(
        remote(&store, provider).await.request_id.as_deref(),
        Some(REQUEST)
    );
    assert_eq!(
        pin_invocation_route::Entity::find_by_id(&b_poll.id)
            .one(store.db())
            .await
            .unwrap()
            .unwrap(),
        b_capture
    );
    assert_eq!(
        pin_invocation_route::Entity::find_by_id(&a_poll.id)
            .one(store.db())
            .await
            .unwrap()
            .unwrap(),
        a_capture
    );
    let usage = pin_provider_usage::Entity::find_by_id(provider)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(usage.reserved_pins, 1);
    assert_eq!(usage.reserved_bytes, 100);
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
        2
    );
}

#[tokio::test]
async fn retired_shared_request_hands_off_cancelled_canonical_poll() {
    handoff(false, false).await;
}

#[tokio::test]
async fn cancellation_before_retirement_hands_off_read_only_poll() {
    handoff(true, false).await;
}

#[tokio::test]
async fn strategy_change_hands_off_original_read_only_route() {
    handoff(false, true).await;
}

async fn change_registered_route(
    store: &Store,
    provider: &str,
    field: &str,
    value: serde_json::Value,
) {
    let row = pin_provider_route::Entity::find_by_id(provider)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let mut snapshot: serde_json::Value = serde_json::from_str(&row.snapshot).unwrap();
    snapshot[field] = value;
    let mut changed: pin_provider_route::ActiveModel = row.into();
    changed.snapshot = Set(snapshot.to_string());
    changed.update(store.db()).await.unwrap();
}

#[derive(Clone, Copy, Debug)]
enum InFlightBarrier {
    None,
    Strategy,
    Credential,
    Endpoint,
    Account,
    Archived,
    NoDesired,
}

async fn cancelled_owner_during_first_post(barrier: InFlightBarrier) {
    let server = MockServer::start().await;
    let (arrived, posted) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    Mock::given(method("POST"))
        .and(path("/pins"))
        .respond_with(move |_: &wiremock::Request| {
            arrived.send(()).unwrap();
            released
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(8))
                .unwrap();
            ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"requestid": REQUEST, "status": "queued", "pin": {"cid": CID}}),
            )
        })
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/pins/{REQUEST}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"requestid": REQUEST, "status": "pinned", "pin": {"cid": CID}}),
        ))
        .mount(&server)
        .await;

    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let store = Store::new(db);
    store::bucket::create(store.db(), "handoff", None)
        .await
        .unwrap();
    let original = runtime(&server.uri(), false, 1);
    original.register_identities(&store).await.unwrap();
    publish(&store, &original, "A").await;
    let a = pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("submit"))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let captured = pin_invocation_route::Entity::find_by_id(&a.id)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let provider = a.provider.clone();
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

    publish(&store, &original, "B").await;
    let b = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::Id.ne(a.target_id.as_ref().unwrap()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let before = remote(&store, &provider).await;
    assert!(before.request_id.is_none());
    assert_eq!(
        pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .all(store.db())
            .await
            .unwrap()
            .len(),
        0,
        "there is no Poll to borrow before the first POST returns"
    );
    let retired = runtime(
        &server.uri(),
        !matches!(barrier, InFlightBarrier::Strategy),
        1,
    );
    retired.register_identities(&store).await.unwrap();
    match barrier {
        InFlightBarrier::None => {}
        InFlightBarrier::Strategy => {
            change_registered_route(&store, &provider, "strategy", serde_json::json!("upload"))
                .await;
        }
        InFlightBarrier::Credential => {
            change_registered_route(
                &store,
                &provider,
                "credential_revision",
                serde_json::json!(2),
            )
            .await;
        }
        InFlightBarrier::Endpoint => {
            change_registered_route(&store, &provider, "endpoint_revision", serde_json::json!(2))
                .await;
        }
        InFlightBarrier::Account => {
            change_registered_route(&store, &provider, "scope", serde_json::json!("other")).await;
        }
        InFlightBarrier::Archived => {
            ledger::archive_confirmed_release(store.db(), &before)
                .await
                .unwrap();
        }
        InFlightBarrier::NoDesired => cancel(&store, &b.lease_id).await,
    }
    cancel(&store, a.lease_id.as_ref().unwrap()).await;
    assert!(remote(&store, &provider).await.epoch > captured.remote_epoch);
    release.send(()).unwrap();
    wait_done(&store, &a.id).await;
    worker.shutdown(Duration::from_secs(3)).await;
    assert_eq!(
        pin_lease::Entity::find_by_id(a.lease_id.as_ref().unwrap())
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "cancelled"
    );
    assert_eq!(
        remote(&store, &provider).await.request_id.as_deref(),
        Some(REQUEST)
    );

    let b_poll = pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("poll"))
        .filter(pin_job::Column::TargetId.eq(&b.id))
        .one(store.db())
        .await
        .unwrap();
    if matches!(barrier, InFlightBarrier::None | InFlightBarrier::Strategy) {
        let poll = b_poll.expect("first queued POST must atomically hand off a Poll to B");
        let poll_route = pin_invocation_route::Entity::find_by_id(&poll.id)
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(poll_route.route, captured.route);
        assert_eq!(
            poll_route.remote_epoch,
            remote(&store, &provider).await.epoch
        );
        let worker = retired.start(store.clone(), CancellationToken::new());
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if pin_lease_target::Entity::find_by_id(&b.id)
                    .one(store.db())
                    .await
                    .unwrap()
                    .unwrap()
                    .state
                    == "pinned"
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("B must converge through the captured request's GET");
        worker.shutdown(Duration::from_secs(3)).await;
        assert_eq!(job(&store, &poll.id).await.state, "done");
    } else {
        assert!(
            b_poll.is_none(),
            "{barrier:?} must fence first-response Poll handoff"
        );
        let worker = original.start(store.clone(), CancellationToken::new());
        if let Some(reconcile) = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("reconcile"))
            .filter(pin_job::Column::ExpectedRemoteEpoch.eq(remote(&store, &provider).await.epoch))
            .one(store.db())
            .await
            .unwrap()
        {
            wait_done(&store, &reconcile.id).await;
        }
        worker.shutdown(Duration::from_secs(3)).await;
    }
    let usage = pin_provider_usage::Entity::find_by_id(&provider)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_pins, usage.reserved_bytes), (1, 100));
    assert_eq!(
        pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .all(store.db())
            .await
            .unwrap()
            .len(),
        1,
        "handoff cannot allocate a replacement Submit"
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
        if matches!(barrier, InFlightBarrier::None | InFlightBarrier::Strategy) {
            1
        } else {
            0
        },
        "{barrier:?}: only an eligible B may GET the historical request"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.method.as_str() == "DELETE")
            .count(),
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_submit_owner_hands_first_queued_response_to_b_after_retirement() {
    cancelled_owner_during_first_post(InFlightBarrier::None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_submit_owner_hands_first_queued_response_to_b_after_strategy_change() {
    cancelled_owner_during_first_post(InFlightBarrier::Strategy).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_response_handoff_fences_identity_lifetime_and_zero_refs() {
    for barrier in [
        InFlightBarrier::Credential,
        InFlightBarrier::Endpoint,
        InFlightBarrier::Account,
        InFlightBarrier::Archived,
        InFlightBarrier::NoDesired,
    ] {
        cancelled_owner_during_first_post(barrier).await;
    }
}

#[derive(Clone, Copy, Debug)]
enum Barrier {
    Credential,
    Endpoint,
    Account,
    Archived,
    Request,
    NoDesired,
    CredentialAfterHandoff,
}

async fn blocked_handoff(barrier: Barrier) {
    let server = MockServer::start().await;
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let store = Store::new(db);
    let (original, a, b) = queued_pair(&store, &server).await;
    let provider = &a.provider;
    let before = remote(&store, provider).await;
    let retired = runtime(&server.uri(), true, 1);
    retired.register_identities(&store).await.unwrap();
    match barrier {
        Barrier::Credential => {
            change_registered_route(
                &store,
                provider,
                "credential_revision",
                serde_json::json!(2),
            )
            .await
        }
        Barrier::Endpoint => {
            change_registered_route(&store, provider, "endpoint_revision", serde_json::json!(2))
                .await
        }
        Barrier::Account => {
            change_registered_route(
                &store,
                provider,
                "scope",
                serde_json::json!("different-account"),
            )
            .await
        }
        Barrier::Archived => ledger::archive_confirmed_release(store.db(), &before)
            .await
            .unwrap(),
        Barrier::Request => {
            let mut changed: remote_pin::ActiveModel = before.clone().into();
            changed.request_id = Set(Some("different-request".into()));
            changed.update(store.db()).await.unwrap();
        }
        Barrier::NoDesired => cancel(&store, &b.lease_id).await,
        Barrier::CredentialAfterHandoff => {}
    }
    cancel(&store, a.lease_id.as_ref().unwrap()).await;
    let after = remote(&store, provider).await;
    let reconcile = pin_job::Entity::find()
        .filter(pin_job::Column::ExpectedRemoteEpoch.eq(after.epoch))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let worker = retired.start(store.clone(), CancellationToken::new());
    wait_done(&store, &reconcile.id).await;
    worker.shutdown(Duration::from_secs(3)).await;
    let b_poll = pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("poll"))
        .filter(pin_job::Column::TargetId.eq(&b.id))
        .one(store.db())
        .await
        .unwrap();
    let checked = if matches!(barrier, Barrier::CredentialAfterHandoff) {
        let b_poll = b_poll.expect("handoff must exist before credential rotation");
        runtime(&server.uri(), true, 2)
            .register_identities(&store)
            .await
            .unwrap();
        b_poll
    } else {
        assert!(
            b_poll.is_none(),
            "{barrier:?} must not transfer a historical Poll"
        );
        a.clone()
    };
    // Keep the old runtime alive: persisted route changes must fence even a
    // worker that still has the former credential/endpoint in memory.
    let worker = original.start(store.clone(), CancellationToken::new());
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let row = job(&store, &checked.id).await;
            if row.state == "running" && row.locked_until.is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("invalid historical Poll must park without HTTP");
    worker.shutdown(Duration::from_secs(3)).await;
    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "{barrier:?}: no GET, POST retry, or DELETE permitted: {requests:?}"
    );
    assert_eq!(remote(&store, provider).await.epoch, after.epoch);
    let usage = pin_provider_usage::Entity::find_by_id(provider)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_pins, usage.reserved_bytes), (1, 100));
    assert_eq!(
        pin_lease::Entity::find_by_id(a.lease_id.as_ref().unwrap())
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "cancelled"
    );
}

#[tokio::test]
async fn handoff_rejects_changed_account_credentials_and_endpoint() {
    for barrier in [Barrier::Credential, Barrier::Endpoint, Barrier::Account] {
        blocked_handoff(barrier).await;
    }
}

#[tokio::test]
async fn handoff_rejects_archived_lifetime_changed_request_and_zero_desired() {
    for barrier in [Barrier::Archived, Barrier::Request, Barrier::NoDesired] {
        blocked_handoff(barrier).await;
    }
}

#[tokio::test]
async fn transferred_poll_cannot_call_after_credentials_change() {
    blocked_handoff(Barrier::CredentialAfterHandoff).await;
}

mod postgres {
    use super::*;
    use futures_util::FutureExt;
    use sea_orm::{
        ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement,
    };

    async fn wait_sql(db: &DatabaseConnection, sql: String, checkpoint: &str) {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let reached: bool = db
                    .query_one(Statement::from_string(
                        DatabaseBackend::Postgres,
                        sql.clone(),
                    ))
                    .await
                    .unwrap()
                    .unwrap()
                    .try_get("", "reached")
                    .unwrap();
                if reached {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("missing PG checkpoint: {checkpoint}"));
    }

    async fn branch(
        control: &DatabaseConnection,
        store: &Store,
        other: &Store,
        schema: &str,
        cancellation_first: bool,
    ) {
        let server = MockServer::start().await;
        store::run_migrations(store.db()).await.unwrap();
        let (_, a, b) = queued_pair(store, &server).await;
        let retired = runtime(&server.uri(), true, 1);
        retired.register_identities(store).await.unwrap();
        cancel(store, a.lease_id.as_ref().unwrap()).await;
        let handoff_epoch = remote(store, &a.provider).await.epoch;
        let lock_key = i64::from_be_bytes(uuid::Uuid::new_v4().as_bytes()[..8].try_into().unwrap());
        let control_pid: i64 = control
            .query_one(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT pg_backend_pid()::bigint AS value",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "value")
            .unwrap();
        // Forward: pause handoff at INSERT after ordered lease/target/remote
        // locks. Reverse: pause cancellation after its lease CAS holds B's lock.
        let trigger = if cancellation_first {
            format!(
                "CREATE FUNCTION {schema}.gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state='cancelled' AND NEW.id='{}' THEN PERFORM pg_advisory_xact_lock({lock_key}); END IF; RETURN NEW; END $$; CREATE TRIGGER gate AFTER UPDATE ON {schema}.pin_leases FOR EACH ROW EXECUTE FUNCTION {schema}.gate()",
                b.lease_id
            )
        } else {
            format!(
                "CREATE FUNCTION {schema}.gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.operation='poll' AND NEW.target_id='{}' THEN PERFORM pg_advisory_xact_lock({lock_key}); END IF; RETURN NEW; END $$; CREATE TRIGGER gate BEFORE INSERT ON {schema}.pin_jobs FOR EACH ROW EXECUTE FUNCTION {schema}.gate()",
                b.id
            )
        };
        control.execute_unprepared(&trigger).await.unwrap();
        control
            .execute_unprepared(&format!("SELECT pg_advisory_lock({lock_key})"))
            .await
            .unwrap();
        let mut cancellation = None;
        let mut worker = None;
        let result = std::panic::AssertUnwindSafe(async {
            if cancellation_first {
                let store = other.clone();
                let lease_id = b.lease_id.clone();
                cancellation = Some(tokio::spawn(async move { cancel(&store, &lease_id).await }));
            } else {
                worker = Some(retired.start(store.clone(), CancellationToken::new()));
            }
            wait_sql(control, format!("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE {control_pid}=ANY(pg_blocking_pids(pid)) AND wait_event='advisory') AS reached"), "first transaction at trigger").await;
            let first_pid: i64 = control.query_one(Statement::from_string(DatabaseBackend::Postgres, format!("SELECT pid::bigint AS value FROM pg_stat_activity WHERE {control_pid}=ANY(pg_blocking_pids(pid)) AND wait_event='advisory'"))).await.unwrap().unwrap().try_get("", "value").unwrap();
            if cancellation_first {
                worker = Some(retired.start(store.clone(), CancellationToken::new()));
            } else {
                let store = other.clone();
                let lease_id = b.lease_id.clone();
                cancellation = Some(tokio::spawn(async move { cancel(&store, &lease_id).await }));
            }
            wait_sql(control, format!("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE {first_pid}=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock' AND query ILIKE '%pin_leases%') AS reached"), "opposing lifecycle transaction waits on B lease").await;
            control.execute_unprepared(&format!("SELECT pg_advisory_unlock({lock_key})")).await.unwrap();
            tokio::time::timeout(Duration::from_secs(8), cancellation.as_mut().unwrap()).await.unwrap().unwrap();
            cancellation = None;
            let after = remote(store, &a.provider).await;
            assert_eq!(after.epoch, handoff_epoch + 1);
            assert_eq!(after.request_id.as_deref(), Some(REQUEST));
            let b_poll = pin_job::Entity::find().filter(pin_job::Column::Operation.eq("poll")).filter(pin_job::Column::TargetId.eq(&b.id)).one(store.db()).await.unwrap();
            if cancellation_first {
                assert!(b_poll.is_none(), "handoff cannot project an unlocked stale desired snapshot");
            } else {
                let b_poll = b_poll.expect("handoff committed before cancellation acquired its lifecycle lock");
                let captured = pin_invocation_route::Entity::find_by_id(&b_poll.id).one(store.db()).await.unwrap().unwrap();
                assert_eq!(captured.remote_epoch, handoff_epoch);
                assert!(!jobs::check_target_job_generation(store.db(), &b_poll).await.unwrap());
            }
            // Let both historical target jobs reach their due scan. Neither may
            // GET after B's cancellation, regardless of transaction ordering.
            tokio::time::timeout(Duration::from_secs(8), async {
                loop {
                    let polls = pin_job::Entity::find().filter(pin_job::Column::Operation.eq("poll")).all(store.db()).await.unwrap();
                    if polls.iter().all(|poll| poll.state == "running" && poll.locked_until.is_none()) { break; }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }).await.expect("zero-ref historical polls park");
            let usage = pin_provider_usage::Entity::find_by_id(&a.provider).one(store.db()).await.unwrap().unwrap();
            assert_eq!((usage.reserved_pins, usage.reserved_bytes), (1, 100));
            assert_eq!(server.received_requests().await.unwrap().len(), 1);
            let targets = pin_lease_target::Entity::find().all(store.db()).await.unwrap();
            assert!(targets.iter().all(|target| target.state == "released"));
        }).catch_unwind().await;
        control
            .execute_unprepared("SELECT pg_advisory_unlock_all()")
            .await
            .unwrap();
        if let Some(task) = cancellation {
            task.abort();
            let _ = task.await;
        }
        if let Some(worker) = worker {
            worker.shutdown(Duration::from_secs(3)).await;
        }
        if let Err(error) = result {
            std::panic::resume_unwind(error);
        }
    }

    async fn exercise(cancellation_first: bool) {
        let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL")
            .expect("explicit local PG test URL required");
        let schema = format!("stage2_handoff_{}", uuid::Uuid::new_v4().simple());
        let mut options = ConnectOptions::new(url.clone());
        options.min_connections(1).max_connections(1);
        let control = Database::connect(options).await.unwrap();
        control
            .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        let result = std::panic::AssertUnwindSafe(async {
            let mut scoped = url::Url::parse(&url).unwrap();
            scoped
                .query_pairs_mut()
                .append_pair("options", &format!("-csearch_path={schema}"));
            let db = store::connect_database(scoped.as_str()).await.unwrap();
            let other_db = store::connect_database(scoped.as_str()).await.unwrap();
            let result = std::panic::AssertUnwindSafe(branch(
                &control,
                &Store::new(db.clone()),
                &Store::new(other_db.clone()),
                &schema,
                cancellation_first,
            ))
            .catch_unwind()
            .await;
            db.close().await.unwrap();
            other_db.close().await.unwrap();
            if let Err(error) = result {
                std::panic::resume_unwind(error);
            }
        })
        .catch_unwind()
        .await;
        control
            .execute_unprepared("SELECT pg_advisory_unlock_all()")
            .await
            .unwrap();
        assert!(
            schema.starts_with("stage2_handoff_")
                && schema
                    .bytes()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == b'_')
        );
        control
            .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
            .await
            .unwrap();
        control.close().await.unwrap();
        if let Err(error) = result {
            std::panic::resume_unwind(error);
        }
    }

    #[tokio::test]
    #[ignore = "requires isolated IPFS_S3_TEST_POSTGRES_URL"]
    async fn handoff_locks_desired_owner_before_concurrent_cancellation() {
        exercise(false).await;
    }

    #[tokio::test]
    #[ignore = "requires isolated IPFS_S3_TEST_POSTGRES_URL"]
    async fn cancellation_locks_owner_before_concurrent_handoff() {
        exercise(true).await;
    }
}
