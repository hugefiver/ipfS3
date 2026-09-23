use ipfs_s3_gateway::{
    config::Config,
    pinning::{config::ValidatedPinningConfig, coordinator::PinningCoordinator},
};

fn config(names: &[(&str, &str)]) -> Config {
    let mut text = String::from("[pinning_identity]\nprimary_storage_domain='local'\n");
    for (name, id) in names {
        text.push_str(&format!("[[pinning_identity.providers]]\nconfig_name='{name}'\nprovider_id='{id}'\ndisplay_name='{name}'\nbackend='noop'\nscope='account'\nstorage_domain='remote'\ncredential_revision=1\nendpoint_revision=1\napi_profile='noop'\nstrategy='cid'\n"));
    }
    for (name, _) in names {
        text.push_str(&format!("[[pinning.providers]]\nname='{name}'\nkind='noop'\npriority=1\nmax_bytes=100\nmax_pins=1\n"));
    }
    toml::from_str(&text).unwrap()
}

#[test]
fn same_backend_aliases_share_capacity_identity() {
    let cfg = config(&[("first", "stable-one"), ("second", "stable-two")]);
    let coordinator =
        PinningCoordinator::build(ValidatedPinningConfig::from_config(&cfg, |_| None).unwrap())
            .unwrap();
    assert_eq!(coordinator.provider_limits().len(), 1);
}

#[test]
fn rename_keeps_capacity_identity() {
    let first = PinningCoordinator::build(
        ValidatedPinningConfig::from_config(&config(&[("old", "stable")]), |_| None).unwrap(),
    )
    .unwrap();
    let second = PinningCoordinator::build(
        ValidatedPinningConfig::from_config(&config(&[("new", "stable")]), |_| None).unwrap(),
    )
    .unwrap();
    assert_eq!(first.provider_limits(), second.provider_limits());
}

use chrono::Utc;
use ipfs_s3_gateway::{
    pinning::{
        config::{LeaseDuration, ProviderMode},
        policy::{LeaseIntent, LeaseSource, PublicationPolicy},
        tags::ContentMode,
    },
    store::{
        self, Store,
        entities::{pin_job, pin_lease, pin_lease_target, pin_provider_route, remote_pin},
        pinning::{
            leases, ledger,
            publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
            quota,
        },
    },
};
use sea_orm::{ColumnTrait, ConnectionTrait, Database, EntityTrait, QueryFilter, TransactionTrait};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

fn psa_config(endpoint: &str, scope: &str, cleanup: &str, names: &[(&str, &str)]) -> Config {
    let mut cfg = config(names);
    for provider in &mut cfg.pinning.providers {
        provider.kind = "filebase".into();
        provider.token_env = Some("STAGE2_TOKEN".into());
        provider.endpoint = Some(endpoint.into());
        provider.requests_per_second = Some(100);
        provider.max_bytes = 1000;
    }
    cfg.pinning.worker_interval = "1s".into();
    for identity in &mut cfg.pinning_identity.providers {
        identity.backend = "filebase".into();
        identity.api_profile = "filebase-psa".into();
        identity.secret_ref = Some("env:STAGE2_TOKEN".into());
        identity.scope = scope.into();
        identity.cleanup = if cleanup == "managed" {
            ipfs_s3_gateway::pinning::identity::CleanupMode::Managed
        } else {
            ipfs_s3_gateway::pinning::identity::CleanupMode::Retain
        };
    }
    cfg
}

fn coordinator(cfg: &Config) -> Arc<PinningCoordinator> {
    PinningCoordinator::build(
        ValidatedPinningConfig::from_config(cfg, |_| Some("test-only-token".into())).unwrap(),
    )
    .unwrap()
}

async fn publish(store: &Store, coordinator: &PinningCoordinator, key: &str, cid: &str) {
    publish_for_duration(store, coordinator, key, cid, "1h").await;
}

async fn publish_for_duration(
    store: &Store,
    coordinator: &PinningCoordinator,
    key: &str,
    cid: &str,
    duration: &str,
) {
    if let Err(error) = try_publish(store, coordinator, key, cid, duration).await {
        let provider = coordinator.provider_limits().keys().next().unwrap();
        let remote = remote_pin::Entity::find_by_id((provider.clone(), cid.to_owned()))
            .one(store.db())
            .await;
        let historical = ledger::get(store.db(), provider, cid).await;
        let current = pin_provider_route::Entity::find_by_id(provider.clone())
            .one(store.db())
            .await;
        // Diagnostic only: never print snapshots, tokens, URLs or secret references.
        let remote_state = remote
            .as_ref()
            .ok()
            .and_then(|row| row.as_ref())
            .map(|row| (row.status.as_str(), row.epoch));
        let ledger_route_present = historical
            .as_ref()
            .ok()
            .and_then(|row| row.as_ref())
            .map(|row| row.route.is_some());
        let route_state = current
            .as_ref()
            .ok()
            .and_then(|row| row.as_ref())
            .map(|row| (row.retired, row.snapshot.as_str()));
        let route_matches_ledger = route_state.and_then(|(_, snapshot)| {
            historical
                .as_ref()
                .ok()
                .and_then(|row| row.as_ref())
                .map(|row| row.route.as_deref() == Some(snapshot))
        });
        panic!(
            "publish {key}/{cid} failed: {error}; provider={provider}; remote={remote_state:?}; remote_query_ok={}; ledger_route_present={ledger_route_present:?}; ledger_query_ok={}; current_route_retired={:?}; current_route_query_ok={}; route_matches_ledger={route_matches_ledger:?}",
            remote.is_ok(),
            historical.is_ok(),
            route_state.map(|(retired, _)| retired),
            current.is_ok(),
        );
    }
}

async fn try_publish(
    store: &Store,
    coordinator: &PinningCoordinator,
    key: &str,
    cid: &str,
    duration: &str,
) -> ipfs_s3_gateway::error::AppResult<publication::PublicationResult> {
    let provider = coordinator.provider_limits().keys().next().unwrap().clone();
    publication::publish_object(
        store.db(),
        PublicationRequest {
            object: PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                "stage2",
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
                    policy_id: "stage2".into(),
                    provider_mode: ProviderMode::All,
                    providers: vec![provider],
                    content_mode: ContentMode::Object,
                    duration: LeaseDuration::parse(duration).unwrap(),
                }],
            },
            object_target: PinTargetSpec {
                cid: cid.into(),
                logical_size: 100,
            },
        },
        coordinator.provider_limits(),
    )
    .await
}

async fn wait_for<F, Fut>(label: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if condition().await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("stage2 durable condition did not converge: {label}"));
}

async fn remote_is(store: &Store, provider: &str, cid: &str, status: &str) -> bool {
    remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(store.db())
        .await
        .unwrap()
        .is_some_and(|r| r.status == status)
}

async fn wait_for_effect(store: &Store, provider: &str, cid: &str, effect: &str) {
    let completed = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if ledger::get(store.db(), provider, cid)
                .await
                .unwrap()
                .unwrap()
                .effect
                == effect
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok();
    if !completed {
        let jobs = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq(provider))
            .filter(pin_job::Column::Cid.eq(cid))
            .all(store.db())
            .await
            .unwrap();
        panic!(
            "expected {effect}; ledger={:?}; snapshot={:?}; jobs={jobs:?}",
            ledger::get(store.db(), provider, cid).await.unwrap(),
            leases::remote_work_snapshot(store.db(), provider, cid)
                .await
                .unwrap()
        );
    }
}

async fn lease_rows(store: &Store, provider: &str) -> Vec<pin_lease::Model> {
    let targets = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::Provider.eq(provider))
        .all(store.db())
        .await
        .unwrap();
    pin_lease::Entity::find()
        .filter(pin_lease::Column::Id.is_in(targets.into_iter().map(|t| t.lease_id)))
        .all(store.db())
        .await
        .unwrap()
}

async fn end_lease_with_transaction_retry(store: &Store, cancel: Option<&str>) {
    for attempt in 0..8 {
        let txn = store.db().begin().await.unwrap();
        let result = match cancel {
            Some(id) => leases::cancel_lease(&txn, id, Utc::now()).await.map(|_| ()),
            None => leases::expire_due_leases(&txn, Utc::now())
                .await
                .map(|_| ()),
        };
        match result {
            Ok(()) => {
                txn.commit().await.unwrap();
                return;
            }
            Err(ipfs_s3_gateway::error::AppError::Database(message))
                if message == "stale lease lifecycle compare-and-set" && attempt < 7 =>
            {
                txn.rollback().await.unwrap();
            }
            Err(error) => panic!("unexpected lifecycle failure: {error}"),
        }
    }
    unreachable!();
}

async fn exercise_stage2(db: sea_orm::DatabaseConnection) {
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "stage2", None).await.unwrap();
    let store = Store::new(db);
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/pins")).respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"requestid":"shared-request","status":"pinned","pin":{"cid":"bafy-shared"}}))).expect(1).mount(&server).await;
    let cfg = psa_config(
        &server.uri(),
        "shared",
        "retain",
        &[("old", "stable-one"), ("alias", "stable-two")],
    );
    let initial = coordinator(&cfg);
    initial.register_identities(&store).await.unwrap();
    let provider = initial.provider_limits().keys().next().unwrap().clone();
    // Independent concurrent object publications contend on the same remote and
    // usage rows on both engines; aliases cannot create extra capacity.
    tokio::join!(
        publish(&store, &initial, "first", "bafy-shared"),
        publish(&store, &initial, "second", "bafy-shared")
    );
    assert_eq!(
        quota::read_usage(store.db(), &provider)
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        1
    );
    assert_eq!(lease_rows(&store, &provider).await.len(), 2);
    let before = lease_rows(&store, &provider).await;
    // Rename before any worker has sent a request.
    let mut renamed = cfg.clone();
    renamed.pinning.providers[0].name = "renamed".into();
    renamed.pinning_identity.providers[0].config_name = "renamed".into();
    renamed.pinning_identity.providers[0].display_name = "New display".into();
    let runtime = coordinator(&renamed);
    runtime.register_identities(&store).await.unwrap();
    assert!(
        ledger::route_matches(
            store.db(),
            &provider,
            "bafy-shared",
            runtime.provider_identity(&provider)
        )
        .await
        .unwrap()
    );
    let worker = runtime.start(store.clone(), CancellationToken::new());
    wait_for("shared pinned", || {
        remote_is(&store, &provider, "bafy-shared", "pinned")
    })
    .await;
    let status = ledger::status(store.db(), &provider, "bafy-shared")
        .await
        .unwrap()
        .unwrap();
    // A PSA request ID alone does not prove exclusive ownership of an existing CID.
    assert_eq!(status.ownership, ledger::Ownership::Unknown);
    assert!(status.remote_pinned_at.is_some());
    assert!(status.content_verified_at.is_none());
    for old in &before {
        let current = pin_lease::Entity::find_by_id(old.id.clone())
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.created_at, old.created_at);
        assert_eq!(current.expires_at, old.expires_at);
        let layered = ledger::lease_status(store.db(), &old.id)
            .await
            .unwrap()
            .unwrap();
        assert!(layered.local_metadata_published);
        assert!(layered.any_provider_available);
        assert!(layered.all_targets_pinned);
    }
    // Cancellation of one owner and expiry of the other race without releasing
    // the retained resource or deleting the shared request.
    let cancel = end_lease_with_transaction_retry(&store, Some(&before[0].id));
    let expire = end_lease_with_transaction_retry(&store, None);
    // Make the fixture due without passing a future scheduling clock to the
    // real worker. Cancel/expire still contend on the same live lifecycle rows.
    pin_lease::Entity::update_many()
        .col_expr(
            pin_lease::Column::ExpiresAt,
            sea_orm::sea_query::Expr::value(Utc::now() - chrono::Duration::seconds(1)),
        )
        .filter(pin_lease::Column::Id.is_in(before.iter().map(|l| l.id.clone())))
        .exec(store.db())
        .await
        .unwrap();
    tokio::join!(cancel, expire);
    wait_for_effect(&store, &provider, "bafy-shared", "retained").await;
    assert_eq!(
        quota::read_usage(store.db(), &provider)
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        1
    );
    for index in 0..3 {
        publish(
            &store,
            &runtime,
            &format!("blocked-{index}"),
            &format!("bafy-blocked-{index}"),
        )
        .await;
        let txn = store.db().begin().await.unwrap();
        pin_lease::Entity::update_many()
            .col_expr(
                pin_lease::Column::ExpiresAt,
                sea_orm::sea_query::Expr::value(Utc::now() - chrono::Duration::seconds(1)),
            )
            .filter(pin_lease::Column::State.eq("active"))
            .exec(&txn)
            .await
            .unwrap();
        leases::expire_due_leases(&txn, Utc::now()).await.unwrap();
        txn.commit().await.unwrap();
    }
    assert_eq!(
        quota::read_usage(store.db(), &provider)
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        1
    );
    worker.shutdown(Duration::from_secs(5)).await;
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
            .filter(|r| r.method.as_str() == "DELETE")
            .count(),
        0
    );

    // Every protected revision boundary is fail-closed against the old snapshot.
    for field in ["scope", "credential", "endpoint", "missing"] {
        let mut changed = renamed.clone();
        match field {
            "scope" => {
                for id in &mut changed.pinning_identity.providers {
                    id.scope = "new-account".into();
                }
            }
            "credential" => {
                for id in &mut changed.pinning_identity.providers {
                    id.credential_revision += 1;
                }
            }
            "endpoint" => {
                for id in &mut changed.pinning_identity.providers {
                    id.endpoint_revision += 1;
                }
            }
            _ => {
                changed.pinning.providers.clear();
                changed.pinning_identity.providers.clear();
            }
        }
        let changed = coordinator(&changed);
        assert!(
            !ledger::route_matches(
                store.db(),
                &provider,
                "bafy-shared",
                changed.provider_identity(&provider)
            )
            .await
            .unwrap(),
            "{field}"
        );
    }
    let mut retired = renamed.clone();
    for id in &mut retired.pinning_identity.providers {
        id.retired = true;
    }
    let retired = coordinator(&retired);
    retired.register_identities(&store).await.unwrap();
    assert!(
        try_publish(&store, &retired, "retired-rejected", "bafy-retired", "1h")
            .await
            .is_err()
    );
    assert!(
        remote_pin::Entity::find_by_id((provider.clone(), "bafy-retired".into()))
            .one(store.db())
            .await
            .unwrap()
            .is_none()
    );
    assert!(!retired.provider_limits()[&provider].enabled);
    assert!(retired.provider(&provider).is_some());
    assert!(
        ledger::route_matches(
            store.db(),
            &provider,
            "bafy-shared",
            retired.provider_identity(&provider)
        )
        .await
        .unwrap()
    );

    exercise_cleanup_failure_with_attested_ownership(&store).await;
    exercise_external_existing(&store).await;
    exercise_late_success(&store).await;
    exercise_identity_isolation(&store).await;
}

async fn upgrade_old_fixture(db: &sea_orm::DatabaseConnection) {
    use sea_orm_migration::{MigrationTrait, SchemaManager};
    // The isolated database is empty. Removing only the new tables recreates
    // the actual prior schema, then old persisted remote/history rows are added.
    db.execute_unprepared("DROP TABLE remote_pin_ledger")
        .await
        .unwrap();
    db.execute_unprepared("DROP TABLE pin_invocation_routes")
        .await
        .unwrap();
    db.execute_unprepared("DROP TABLE pin_resource_history")
        .await
        .unwrap();
    db.execute_unprepared("DROP TABLE pin_provider_routes")
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO remote_pins (provider,cid,request_id,cid_size,status,epoch,failure_attempts,last_touched_at) VALUES ('old-fixture','bafy-old','psa-old-request',100,'pinned',7,0,'2026-09-19T00:00:00Z')").await.unwrap();
    db.execute_unprepared("INSERT INTO pin_submit_history (job_id,api,strategy,effect,state,started_at) VALUES ('old-history','psa','cid','created','settled','2026-09-19T00:00:00Z')").await.unwrap();
    db.execute_unprepared("INSERT INTO pin_jobs (id,operation,provider,cid,expected_remote_epoch,state,next_attempt_at,submit_phase) VALUES ('old-unpin','unpin','old-fixture','bafy-old',7,'pending','2026-09-19T00:00:00Z',NULL)").await.unwrap();
    let migration = store::migrations::m20260920_000002_pin_identity_ledger::Migration;
    migration.up(&SchemaManager::new(db)).await.unwrap();
    let row = remote_pin::Entity::find_by_id(("old-fixture".to_owned(), "bafy-old".to_owned()))
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.request_id.as_deref(), Some("psa-old-request"));
    assert_eq!(row.epoch, 7);
    let evidence = ledger::get(db, "old-fixture", "bafy-old")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(evidence.ownership, "unknown");
    assert!(evidence.route.is_none());
    assert!(
        !ledger::cleanup_allowed(db, "old-fixture", "bafy-old")
            .await
            .unwrap()
    );
    assert!(
        !ledger::route_matches(db, "old-fixture", "bafy-old", None)
            .await
            .unwrap()
    );
    assert_eq!(
        store::pinning::jobs::submission_history(db, "old-history")
            .await
            .unwrap()
            .unwrap()
            .api,
        "psa"
    );
    let old_job = pin_job::Entity::find_by_id("old-unpin")
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old_job.state, "running");
    assert!(old_job.locked_until.is_none());
    assert!(migration.down(&SchemaManager::new(db)).await.is_err());
}

async fn exercise_identity_isolation(store: &Store) {
    for field in ["scope", "credential", "endpoint", "missing"] {
        let server = MockServer::start().await;
        let cfg = psa_config(
            &server.uri(),
            &format!("isolation-{field}"),
            "retain",
            &[("isolated", "isolated")],
        );
        let initial = coordinator(&cfg);
        initial.register_identities(store).await.unwrap();
        let provider = initial.provider_limits().keys().next().unwrap().clone();
        let cid = format!("bafy-isolated-{field}");
        publish(store, &initial, &format!("isolated-{field}"), &cid).await;
        let mut changed = cfg;
        match field {
            "scope" => changed.pinning_identity.providers[0].scope = "replacement".into(),
            "credential" => changed.pinning_identity.providers[0].credential_revision += 1,
            "endpoint" => changed.pinning_identity.providers[0].endpoint_revision += 1,
            _ => {
                changed.pinning_identity.providers.clear();
                changed.pinning.providers.clear();
            }
        }
        let changed = coordinator(&changed);
        changed.register_identities(store).await.unwrap();
        let worker = changed.start(store.clone(), CancellationToken::new());
        wait_for(field, || async {
            pin_job::Entity::find()
                .filter(pin_job::Column::Provider.eq(&provider))
                .filter(pin_job::Column::Operation.eq("submit"))
                .one(store.db())
                .await
                .unwrap()
                .is_some_and(|j| {
                    j.locked_until.is_none()
                        && j.last_error.as_deref()
                            == Some("historical identity unavailable; needs_attention")
                })
        })
        .await;
        worker.shutdown(Duration::from_secs(5)).await;
        assert!(
            server.received_requests().await.unwrap().is_empty(),
            "{field} must perform no network requests"
        );
        assert_eq!(
            quota::read_usage(store.db(), &provider)
                .await
                .unwrap()
                .unwrap()
                .reserved_pins,
            1
        );
    }
}

async fn exercise_cleanup_failure_with_attested_ownership(store: &Store) {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/pins")).respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"requestid":"managed","status":"pinned","pin":{"cid":"bafy-managed"}}))).expect(1).mount(&server).await;
    Mock::given(method("DELETE"))
        .and(path("/pins/managed"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let runtime = coordinator(&psa_config(
        &server.uri(),
        "managed",
        "managed",
        &[("managed", "managed")],
    ));
    runtime.register_identities(store).await.unwrap();
    let provider = runtime.provider_limits().keys().next().unwrap().clone();
    publish(store, &runtime, "managed", "bafy-managed").await;
    let worker = runtime.start(store.clone(), CancellationToken::new());
    wait_for("managed pinned", || {
        remote_is(store, &provider, "bafy-managed", "pinned")
    })
    .await;
    // This tests the managed cleanup ledger once ownership is independently
    // attested. The PSA POST response itself does not supply that proof.
    ledger::observe(
        store.db(),
        &provider,
        "bafy-managed",
        ipfs_s3_gateway::pinning::provider::RemotePinStatus::Pinned,
        ledger::Ownership::ApplicationCreated,
        Utc::now(),
    )
    .await
    .unwrap();
    let lease = lease_rows(store, &provider).await.remove(0);
    let txn = store.db().begin().await.unwrap();
    leases::cancel_lease(&txn, &lease.id, Utc::now())
        .await
        .unwrap();
    txn.commit().await.unwrap();
    wait_for("cleanup attempted", || async {
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|r| r.method.as_str() == "DELETE")
    })
    .await;
    wait_for("cleanup failure persisted", || async {
        ledger::get(store.db(), &provider, "bafy-managed")
            .await
            .unwrap()
            .unwrap()
            .last_error
            .is_some()
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    assert_eq!(
        quota::read_usage(store.db(), &provider)
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        1
    );
    assert!(remote_is(store, &provider, "bafy-managed", "pinned").await);
    let ledger = ledger::get(store.db(), &provider, "bafy-managed")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ledger.effect, "cleanup_pending");
    assert!(ledger.first_error.is_some());
    assert!(ledger.last_error.is_some());
    let submit = pin_job::Entity::find()
        .filter(pin_job::Column::Provider.eq(&provider))
        .filter(pin_job::Column::Operation.eq("submit"))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let historical = store::pinning::ledger::invocation_snapshot(store.db(), &submit.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(historical.route.as_ref().unwrap().credential_revision, 1);
    Mock::given(method("DELETE"))
        .and(path("/pins/managed"))
        .respond_with(ResponseTemplate::new(204))
        .with_priority(1)
        .expect(1)
        .mount(&server)
        .await;
    let worker = runtime.start(store.clone(), CancellationToken::new());
    wait_for("managed confirmed absent", || {
        remote_is(store, &provider, "bafy-managed", "absent")
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    assert_eq!(
        quota::read_usage(store.db(), &provider)
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        0
    );
    let archive = store::entities::pin_resource_history::Entity::find()
        .filter(store::entities::pin_resource_history::Column::Provider.eq(&provider))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let archived: serde_json::Value = serde_json::from_str(&archive.ledger).unwrap();
    assert_eq!(archived["request_id"], "managed");
    let mut next = psa_config(
        &server.uri(),
        "managed",
        "managed",
        &[("managed", "managed")],
    );
    next.pinning_identity.providers[0].credential_revision = 2;
    let next = coordinator(&next);
    next.register_identities(store).await.unwrap();
    publish(store, &next, "managed-next-lifetime", "bafy-managed").await;
    assert_eq!(
        store::pinning::ledger::status(store.db(), &provider, "bafy-managed")
            .await
            .unwrap()
            .unwrap()
            .route
            .unwrap()
            .credential_revision,
        2
    );
    assert_eq!(
        store::pinning::ledger::invocation_snapshot(store.db(), &submit.id)
            .await
            .unwrap()
            .unwrap()
            .route
            .unwrap()
            .credential_revision,
        1
    );
}

async fn exercise_external_existing(store: &Store) {
    let server = MockServer::start().await;
    let runtime = coordinator(&psa_config(
        &server.uri(),
        "external",
        "managed",
        &[("external", "external")],
    ));
    runtime.register_identities(store).await.unwrap();
    let provider = runtime.provider_limits().keys().next().unwrap().clone();
    publish(store, &runtime, "external", "bafy-external").await;
    let txn = store.db().begin().await.unwrap();
    leases::apply_remote_status(
        &txn,
        leases::RemoteStatusUpdate {
            provider: &provider,
            cid: "bafy-external",
            request_id: "external-request",
            origin: leases::RemoteStatusOrigin::Adopt,
            status: ipfs_s3_gateway::pinning::provider::RemotePinStatus::Pinned,
            error_class: None,
            error_text: None,
            now: Utc::now(),
        },
    )
    .await
    .unwrap();
    ledger::observe(
        &txn,
        &provider,
        "bafy-external",
        ipfs_s3_gateway::pinning::provider::RemotePinStatus::Pinned,
        ledger::Ownership::ExternalExisting,
        Utc::now(),
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    let lease = lease_rows(store, &provider).await.remove(0);
    let txn = store.db().begin().await.unwrap();
    leases::cancel_lease(&txn, &lease.id, Utc::now())
        .await
        .unwrap();
    txn.commit().await.unwrap();
    let worker = runtime.start(store.clone(), CancellationToken::new());
    wait_for("external retained", || async {
        ledger::get(store.db(), &provider, "bafy-external")
            .await
            .unwrap()
            .unwrap()
            .effect
            == "retained"
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    assert!(
        !ledger::cleanup_allowed(store.db(), &provider, "bafy-external")
            .await
            .unwrap()
    );
    assert!(server.received_requests().await.unwrap().is_empty());
    assert_eq!(
        quota::read_usage(store.db(), &provider)
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        1
    );
}

async fn exercise_late_success(store: &Store) {
    let server = MockServer::start().await;
    let post_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen_post = post_started.clone();
    Mock::given(method("POST")).and(path("/pins")).respond_with(move |_: &wiremock::Request| {
        seen_post.store(true, std::sync::atomic::Ordering::Release);
        ResponseTemplate::new(200).set_delay(Duration::from_millis(1600)).set_body_json(serde_json::json!({"requestid":"late","status":"pinned","pin":{"cid":"bafy-late"}}))
    }).expect(1).mount(&server).await;
    let runtime = coordinator(&psa_config(
        &server.uri(),
        "late",
        "retain",
        &[("late", "late")],
    ));
    runtime.register_identities(store).await.unwrap();
    let provider = runtime.provider_limits().keys().next().unwrap().clone();
    publish_for_duration(store, &runtime, "late", "bafy-late", "1h").await;
    let worker = runtime.start(store.clone(), CancellationToken::new());
    wait_for("late post started", || async {
        post_started.load(std::sync::atomic::Ordering::Acquire)
    })
    .await;
    // Make the existing lease due only after the provider call has started.
    // The worker must accept the late success without changing its deadline.
    let before = lease_rows(store, &provider).await.remove(0);
    pin_lease::Entity::update_many()
        .col_expr(
            pin_lease::Column::ExpiresAt,
            sea_orm::sea_query::Expr::value(Utc::now() - chrono::Duration::seconds(1)),
        )
        .filter(pin_lease::Column::Id.eq(&before.id))
        .exec(store.db())
        .await
        .unwrap();
    let before = lease_rows(store, &provider).await.remove(0);
    end_lease_with_transaction_retry(store, None).await;
    assert_eq!(
        lease_rows(store, &provider).await.remove(0).state,
        "expired"
    );
    assert!(
        !remote_is(store, &provider, "bafy-late", "pinned").await,
        "provider response must arrive after lease expiry"
    );
    wait_for("late pinned", || {
        remote_is(store, &provider, "bafy-late", "pinned")
    })
    .await;
    wait_for("late retained", || async {
        ledger::get(store.db(), &provider, "bafy-late")
            .await
            .unwrap()
            .unwrap()
            .effect
            == "retained"
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
    let after = pin_lease::Entity::find_by_id(before.id)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.state, "expired");
    assert_eq!(after.expires_at, before.expires_at);
    assert_eq!(after.created_at, before.created_at);
    assert_eq!(
        quota::read_usage(store.db(), &provider)
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        1
    );
    assert!(
        ledger::status(store.db(), &provider, "bafy-late")
            .await
            .unwrap()
            .unwrap()
            .remote_pinned_at
            .is_some()
    );
}

#[tokio::test]
async fn sqlite_legacy_unknown_capacity_remains_isolated_after_upgrade() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory
        .path()
        .join("legacy.db")
        .to_string_lossy()
        .replace('\\', "/");
    let db = store::connect_database(&format!("sqlite://{path}?mode=rwc"))
        .await
        .unwrap();
    store::run_migrations(&db).await.unwrap();
    upgrade_old_fixture(&db).await;
    store::bucket::create(&db, "stage2", None).await.unwrap();
    let store = Store::new(db);
    let runtime = coordinator(&psa_config(
        "http://127.0.0.1:1",
        "new-account",
        "retain",
        &[("new", "new")],
    ));
    runtime.register_identities(&store).await.unwrap();
    assert!(
        try_publish(&store, &runtime, "blocked", "bafy-new", "1h")
            .await
            .is_err()
    );
}

#[tokio::test]
#[ignore = "requires isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_legacy_unknown_capacity_remains_isolated_after_upgrade() {
    use futures_util::FutureExt;
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL").unwrap();
    let admin = Database::connect(&url).await.unwrap();
    let schema = format!("stage2_legacy_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let mut scoped = url::Url::parse(&url).unwrap();
    scoped
        .query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let db = store::connect_database(scoped.as_str()).await.unwrap();
    let result = std::panic::AssertUnwindSafe(async {
        store::run_migrations(&db).await.unwrap();
        upgrade_old_fixture(&db).await;
        store::bucket::create(&db, "stage2", None).await.unwrap();
        let store = Store::new(db.clone());
        let runtime = coordinator(&psa_config(
            "http://127.0.0.1:1",
            "new-account",
            "retain",
            &[("new", "new")],
        ));
        runtime.register_identities(&store).await.unwrap();
        assert!(
            try_publish(&store, &runtime, "blocked", "bafy-new", "1h")
                .await
                .is_err()
        );
    })
    .catch_unwind()
    .await;
    db.close().await.unwrap();
    admin
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
async fn sqlite_stage2_concurrent_publication_retain_cleanup_and_late_success() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory
        .path()
        .join("stage2.db")
        .to_string_lossy()
        .replace('\\', "/");
    let db = store::connect_database(&format!("sqlite://{path}?mode=rwc"))
        .await
        .unwrap();
    exercise_stage2(db).await;
}

#[tokio::test]
#[ignore = "requires isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_stage2_concurrent_publication_retain_cleanup_and_late_success() {
    use futures_util::FutureExt;
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL").unwrap();
    let admin = Database::connect(&url).await.unwrap();
    let schema = format!("stage2_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let mut scoped = url::Url::parse(&url).unwrap();
    scoped
        .query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let db = store::connect_database(scoped.as_str()).await.unwrap();
    let result = std::panic::AssertUnwindSafe(exercise_stage2(db.clone()))
        .catch_unwind()
        .await;
    db.close().await.unwrap();
    admin
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
