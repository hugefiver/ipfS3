use super::*;
use crate::{
    pinning::{
        config::ProviderMode,
        identity::{CleanupMode, ProviderRouteSnapshot},
    },
    store::{
        entities::{
            pin_job, pin_lease, pin_lease_requirement, pin_lease_target, pin_provider_route,
            pin_provider_usage, remote_pin, remote_pin_ledger,
        },
        pinning::{
            leases, ledger,
            management::records::{DecodeWork, SharedDecodeBudget},
            quota as store_quota,
        },
    },
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter, Set, sea_query::Expr,
};

const CID: &str = "bafy-one-quota-waiter";
const LEASE_ID: &str = "lease-one-quota-waiter";
const SOURCE_TARGET_ID: &str = "target-one-quota-waiter";

async fn seed_one_quota_waiter(fixture: &mut Fixture) -> chrono::DateTime<Utc> {
    let source = fixture
        .coordinator
        .effective_config()
        .providers
        .iter()
        .find(|provider| provider.identity.display_name == "noop")
        .unwrap()
        .name
        .clone();
    let fast = fixture
        .coordinator
        .effective_config()
        .providers
        .iter()
        .find(|provider| provider.identity.display_name == "fast")
        .unwrap()
        .name
        .clone();
    PinningCoordinator::configure_policy_providers_for_test(
        &mut fixture.coordinator,
        "policy",
        &[&source, &fast],
    );
    for provider in [source.as_str(), fast.as_str()] {
        ledger::register_route(
            fixture.store.db(),
            provider,
            fixture.coordinator.provider_identity(provider).unwrap(),
        )
        .await
        .unwrap();
    }
    let now = Utc::now();
    fixture
        .store
        .db()
        .execute_unprepared(&format!(
            "INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('object-waiter','bucket','one/waiter','{CID}',100,'{CID}'); \
             INSERT INTO pin_leases \
             (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
              last_touched_at, expires_at, generation, state) \
             VALUES ('{LEASE_ID}', 'object-waiter', 'automatic', 'policy', 'one', 'full', '{}', \
                     '{}', '{}', 1, 'active'); \
             INSERT INTO pin_lease_targets \
             (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
             VALUES ('{SOURCE_TARGET_ID}', '{LEASE_ID}', '{CID}', 100, '{source}', 'quota_waiting', \
                     '{}', '{}')",
            now.to_rfc3339(),
            now.to_rfc3339(),
            (now + chrono::Duration::hours(1)).to_rfc3339(),
            now.to_rfc3339(),
            now.to_rfc3339(),
        ))
        .await
        .unwrap();
    pin_lease_requirement::contract::insert(
        fixture.store.db(),
        LEASE_ID,
        "object-waiter",
        "one",
        "full",
        [CID.to_owned()].into_iter(),
        &[source, fast],
        now,
    )
    .await
    .unwrap();
    now
}

#[tokio::test]
async fn invalid_all_waiter_is_terminal_and_independent_provider_advances() {
    let mut fixture = fixture_with_noop_limits([], 100, 1).await;
    let now = seed_one_quota_waiter(&mut fixture).await;
    let mut config = fixture.coordinator.effective_config().clone();
    config.policies[0].provider_mode = ProviderMode::All;
    fixture.coordinator = PinningCoordinator::build(config).unwrap();
    let created = now.to_rfc3339();
    fixture.store.db().execute_unprepared(&format!(
        "UPDATE pin_leases SET provider_mode='all' WHERE id='{LEASE_ID}';
         UPDATE pin_lease_targets SET logical_size=-1 WHERE id='{SOURCE_TARGET_ID}';
         INSERT INTO objects(id,bucket,key,cid,size,etag) VALUES('object-healthy-waiter','bucket','healthy-waiter','healthy-cid',1,'healthy-cid');
         INSERT INTO pin_leases(id,owner_object_id,source,policy_id,provider_mode,content_mode,created_at,last_touched_at,expires_at,generation,state)
         VALUES('lease-healthy-waiter','object-healthy-waiter','automatic','policy','all','full','{created}','{created}','{}',1,'active');
         INSERT INTO pin_lease_targets(id,lease_id,cid,logical_size,provider,state,created_at,last_touched_at)
         VALUES('target-healthy-waiter','lease-healthy-waiter','healthy-cid',1,'fast','quota_waiting','{created}','{created}')",
        (now + chrono::Duration::hours(1)).to_rfc3339(),
    )).await.unwrap();
    super::super::coordinate_quota_waiters(&fixture.coordinator, &fixture.store, now)
        .await
        .unwrap();
    let failed = pin_lease_target::Entity::find_by_id(SOURCE_TARGET_ID)
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.state, "error");
    assert!(failed.last_error.is_some());
    assert_eq!(
        pin_lease_target::Entity::find_by_id("target-healthy-waiter")
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "waiting"
    );
    assert_eq!(
        pin_job::Entity::find()
            .filter(pin_job::Column::TargetId.eq("target-healthy-waiter"))
            .count(fixture.store.db())
            .await
            .unwrap(),
        1
    );
    super::super::coordinate_quota_waiters(&fixture.coordinator, &fixture.store, now)
        .await
        .unwrap();
    assert_eq!(
        pin_lease_target::Entity::find_by_id(SOURCE_TARGET_ID)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "error"
    );
}

#[tokio::test]
async fn missing_current_registration_terminates_waiter_before_allocation() {
    let mut fixture = fixture_with_noop_limits([], 100, 1).await;
    let now = seed_one_quota_waiter(&mut fixture).await;
    pin_provider_route::Entity::delete_by_id("noop")
        .exec(fixture.store.db())
        .await
        .unwrap();
    super::super::quota_waiter::coordinate(&fixture.coordinator, &fixture.store, now)
        .await
        .unwrap();
    assert_eq!(
        pin_lease_target::Entity::find_by_id(SOURCE_TARGET_ID)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "error"
    );
    assert_eq!(
        pin_job::Entity::find()
            .count(fixture.store.db())
            .await
            .unwrap(),
        0
    );
    assert!(
        remote_pin::Entity::find_by_id(("fast".to_owned(), CID.to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn invalid_one_waiter_without_suffix_is_terminal() {
    let mut fixture = fixture_with_noop_limits([], 100, 1).await;
    let now = seed_one_quota_waiter(&mut fixture).await;
    PinningCoordinator::configure_policy_providers_for_test(
        &mut fixture.coordinator,
        "policy",
        &["noop"],
    );
    pin_lease_target::Entity::update_many()
        .col_expr(pin_lease_target::Column::LogicalSize, Expr::value(-1))
        .filter(pin_lease_target::Column::Id.eq(SOURCE_TARGET_ID))
        .exec(fixture.store.db())
        .await
        .unwrap();
    super::super::quota_waiter::coordinate(&fixture.coordinator, &fixture.store, now)
        .await
        .unwrap();
    assert_eq!(
        pin_lease_target::Entity::find_by_id(SOURCE_TARGET_ID)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "error"
    );
}

#[tokio::test]
async fn periodic_quota_maintenance_moves_frozen_one_waiter_to_free_suffix_without_release() {
    let mut fixture = fixture_with_noop_limits([], 100, 1).await;
    let now = seed_one_quota_waiter(&mut fixture).await;

    super::super::quota_waiter::coordinate(&fixture.coordinator, &fixture.store, now)
        .await
        .unwrap();

    let source = pin_lease_target::Entity::find_by_id(SOURCE_TARGET_ID)
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source.state, "released");
    let replacement = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(LEASE_ID))
        .filter(pin_lease_target::Column::Provider.eq("fast"))
        .one(fixture.store.db())
        .await
        .unwrap()
        .expect("the free strict-suffix provider must receive the waiter");
    assert_eq!(replacement.state, "waiting");
    let fast_usage = pin_provider_usage::Entity::find_by_id("fast")
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (fast_usage.reserved_bytes, fast_usage.reserved_pins),
        (100, 1)
    );
    assert_eq!(
        pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("fast"))
            .filter(pin_job::Column::Operation.eq("submit"))
            .count(fixture.store.db())
            .await
            .unwrap(),
        1
    );

    super::super::quota_waiter::coordinate(&fixture.coordinator, &fixture.store, now)
        .await
        .unwrap();
    assert_eq!(
        pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq(LEASE_ID))
            .filter(pin_lease_target::Column::Provider.eq("fast"))
            .count(fixture.store.db())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("fast"))
            .filter(pin_job::Column::Operation.eq("submit"))
            .count(fixture.store.db())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        pin_lease::Entity::find_by_id(LEASE_ID)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .generation,
        2
    );
}

#[tokio::test]
async fn invalid_no_job_waiter_is_terminal_after_policy_restoration() {
    let mut fixture = fixture_with_noop_limits([], 100, 1).await;
    let now = seed_one_quota_waiter(&mut fixture).await;
    pin_lease::Entity::update_many()
        .col_expr(pin_lease::Column::Source, Expr::value("invalid"))
        .filter(pin_lease::Column::Id.eq(LEASE_ID))
        .exec(fixture.store.db())
        .await
        .unwrap();
    super::super::quota_waiter::coordinate(&fixture.coordinator, &fixture.store, now)
        .await
        .unwrap();
    let failed = pin_lease_target::Entity::find_by_id(SOURCE_TARGET_ID)
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.state, "error");
    assert!(failed.last_error.is_some());
    assert_eq!(
        pin_job::Entity::find()
            .count(fixture.store.db())
            .await
            .unwrap(),
        0
    );
    pin_lease::Entity::update_many()
        .col_expr(pin_lease::Column::Source, Expr::value("automatic"))
        .filter(pin_lease::Column::Id.eq(LEASE_ID))
        .exec(fixture.store.db())
        .await
        .unwrap();
    super::super::quota_waiter::coordinate(&fixture.coordinator, &fixture.store, now)
        .await
        .unwrap();
    assert_eq!(
        pin_lease_target::Entity::find_by_id(SOURCE_TARGET_ID)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "error"
    );
    assert_eq!(
        pin_job::Entity::find()
            .count(fixture.store.db())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn quota_waiter_keeps_fifo_source_when_suffix_has_no_capacity_or_operator_debt() {
    for case in ["capacity", "debt"] {
        let mut fixture = fixture_with_noop_limits([], 100, 1).await;
        let now = seed_one_quota_waiter(&mut fixture).await;
        if case == "capacity" {
            fixture
                .store
                .db()
                .execute_unprepared(
                    "INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) \
                     VALUES ('fast',10000,100)",
                )
                .await
                .unwrap();
        } else {
            let mut historical = fixture
                .coordinator
                .provider_identity("fast")
                .unwrap()
                .route_snapshot();
            historical.api_profile = "kubo".into();
            historical.strategy = "cid".into();
            remote_pin_ledger::Entity::insert(remote_pin_ledger::ActiveModel {
                route: Set(Some(serde_json::to_string(&historical).unwrap())),
                provider: Set("fast".to_owned()),
                cid: Set("bafy-unresolved-fast".to_owned()),
                ownership: Set("unknown".to_owned()),
                effect: Set("unknown".to_owned()),
                ..Default::default()
            })
            .exec(fixture.store.db())
            .await
            .unwrap();
            assert_eq!(
                ledger::submission::admission_barrier(fixture.store.db(), "fast")
                    .await
                    .unwrap(),
                ledger::submission::AdmissionBarrier::Operator
            );
        }

        super::super::quota_waiter::coordinate(&fixture.coordinator, &fixture.store, now)
            .await
            .unwrap();

        assert_eq!(
            pin_lease_target::Entity::find_by_id(SOURCE_TARGET_ID)
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "quota_waiting",
            "{case} must retain the FIFO source waiter"
        );
        assert_eq!(
            pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::LeaseId.eq(LEASE_ID))
                .filter(pin_lease_target::Column::Provider.eq("fast"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Provider.eq("fast"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            0
        );
    }
}

#[tokio::test]
async fn quota_waiter_cancelled_after_discovery_cannot_reserve_the_suffix() {
    let mut fixture = fixture_with_noop_limits([], 100, 1).await;
    let now = seed_one_quota_waiter(&mut fixture).await;
    let candidate = store_quota::quota_waiter_page(fixture.store.db(), None, now, 1)
        .await
        .unwrap()
        .candidates
        .pop()
        .unwrap();
    leases::cancel_lease(fixture.store.db(), LEASE_ID, now)
        .await
        .unwrap();

    let session = SharedDecodeBudget::default();
    let work = DecodeWork::shared(&session);
    let tx = fixture.store.begin_runtime(work).await.unwrap();
    assert!(
        leases::fail_runtime_one_quota_waiter(
            &tx,
            LEASE_ID,
            SOURCE_TARGET_ID,
            candidate.lease.generation,
            &["noop".to_owned(), "fast".to_owned()],
            fixture.coordinator.provider_limits(),
            now,
            work,
        )
        .await
        .unwrap()
        .is_none()
    );
    tx.commit().await.unwrap();
    assert!(
        pin_provider_usage::Entity::find_by_id("fast")
            .one(fixture.store.db())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("fast"))
            .count(fixture.store.db())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn worker_tick_pins_one_waiter_on_free_suffix_without_a_release_event() {
    worker_tick_pins_current_candidate(false).await;
}

#[tokio::test]
async fn worker_tick_reorders_unallocated_waiter_to_earlier_current_candidate() {
    worker_tick_pins_current_candidate(true).await;
}

async fn worker_tick_pins_current_candidate(reordered: bool) {
    let mut fixture = fixture_with_noop_limits([], 100, 1).await;
    let blocker = Arc::new(SubmitBlocker {
        entered: Notify::new(),
        release: Notify::new(),
        result: Mutex::new(Some(Ok(remote_for(
            CID,
            "request-fast-quota-waiter",
            RemotePinStatus::Pinned,
        )))),
    });
    let mut fast = ScriptProvider::new([Script::BlockSubmit(blocker.clone())]);
    Arc::get_mut(&mut fast).unwrap().route = ("noop", "cid");
    PinningCoordinator::replace_provider_for_test(&mut fixture.coordinator, "fast", fast.clone());
    seed_one_quota_waiter(&mut fixture).await;
    if reordered {
        let mut config = fixture.coordinator.effective_config().clone();
        config
            .providers
            .iter_mut()
            .find(|provider| provider.name == "fast")
            .unwrap()
            .limits
            .priority = 0;
        config
            .policies
            .iter_mut()
            .find(|policy| policy.prefix == "one/")
            .unwrap()
            .providers = vec!["fast".into(), "noop".into()];
        fixture.coordinator = PinningCoordinator::build(config).unwrap();
        PinningCoordinator::replace_provider_for_test(
            &mut fixture.coordinator,
            "noop",
            fixture.provider.clone(),
        );
        PinningCoordinator::replace_provider_for_test(
            &mut fixture.coordinator,
            "fast",
            fast.clone(),
        );
    }
    assert!(
        remote_pin::Entity::find_by_id(("noop".to_owned(), CID.to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        pin_job::Entity::find()
            .filter(pin_job::Column::TargetId.eq(SOURCE_TARGET_ID))
            .count(fixture.store.db())
            .await
            .unwrap(),
        0
    );
    PinningCoordinator::configure_worker_for_test(&mut fixture.coordinator, |settings| {
        settings.interval = std::time::Duration::from_secs(3600);
    });
    let gate = TickQuotaGate::new(1, false);
    let handle = start_tick_gated_worker(&fixture, gate.clone());
    tick_progress(gate.arrived.notified(), "quota maintenance did not start").await;
    gate.resume.notify_one();
    tick_progress(
        gate.completed.notified(),
        "quota maintenance did not finish",
    )
    .await;
    tick_progress(
        blocker.entered.notified(),
        "free suffix did not receive a Submit",
    )
    .await;
    let mut completed = handle.completed.clone();
    blocker.release.notify_one();
    tick_progress(
        async { completed.changed().await.unwrap() },
        "free suffix Submit did not complete",
    )
    .await;

    let replacement = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(LEASE_ID))
        .filter(pin_lease_target::Column::Provider.eq("fast"))
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replacement.state, "pinned");
    assert_eq!(fast.submits.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 0);
    assert!(
        remote_pin::Entity::find_by_id(("noop".to_owned(), CID.to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        pin_lease_target::Entity::find_by_id(SOURCE_TARGET_ID)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "released"
    );
    let fast_usage = pin_provider_usage::Entity::find_by_id("fast")
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (fast_usage.reserved_bytes, fast_usage.reserved_pins),
        (100, 1)
    );
    let noop_usage = pin_provider_usage::Entity::find_by_id("noop")
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (noop_usage.reserved_bytes, noop_usage.reserved_pins),
        (100, 1)
    );
    handle.shutdown(std::time::Duration::from_secs(1)).await;
}

#[tokio::test]
async fn waiter_reordering_rejects_allocated_resource_or_current_generation_job() {
    for current_job in [false, true] {
        let mut fixture = fixture_with_noop_limits([], 100, 1).await;
        let now = seed_one_quota_waiter(&mut fixture).await;
        let mut original_limits = fixture.coordinator.provider_limits().clone();
        let original = original_limits.get_mut("noop").unwrap();
        original.max_bytes = 200;
        original.max_pins = 2;
        assert_eq!(
            store_quota::reserve_unique(
                fixture.store.db(),
                "noop",
                CID,
                100,
                &original_limits,
                now
            )
            .await
            .unwrap(),
            store_quota::ReservationOutcome::Reserved
        );
        if current_job {
            pin_lease_target::Entity::update_many()
                .col_expr(pin_lease_target::Column::State, Expr::value("waiting"))
                .filter(pin_lease_target::Column::Id.eq(SOURCE_TARGET_ID))
                .exec(fixture.store.db())
                .await
                .unwrap();
            jobs::enqueue_job(
                fixture.store.db(),
                jobs::submit_job("noop", CID, LEASE_ID, SOURCE_TARGET_ID, 1, now),
            )
            .await
            .unwrap();
            pin_lease_target::Entity::update_many()
                .col_expr(
                    pin_lease_target::Column::State,
                    Expr::value("quota_waiting"),
                )
                .filter(pin_lease_target::Column::Id.eq(SOURCE_TARGET_ID))
                .exec(fixture.store.db())
                .await
                .unwrap();
            let tx = fixture.store.db().begin().await.unwrap();
            assert_eq!(
                store_quota::confirmed_release(&tx, "noop", CID, 1, now)
                    .await
                    .unwrap(),
                store_quota::ConfirmedReleaseOutcome::Released
            );
            tx.commit().await.unwrap();
        }
        let before = remote_pin::Entity::find_by_id(("noop".to_owned(), CID.to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        let budget = SharedDecodeBudget::default();
        let work = DecodeWork::shared(&budget);
        let tx = fixture.store.begin_runtime(work).await.unwrap();
        assert!(
            leases::fail_runtime_one_quota_waiter(
                &tx,
                LEASE_ID,
                SOURCE_TARGET_ID,
                1,
                &["noop".into(), "fast".into()],
                fixture.coordinator.provider_limits(),
                now,
                work
            )
            .await
            .unwrap()
            .is_none()
        );
        tx.commit().await.unwrap();
        assert_eq!(
            remote_pin::Entity::find_by_id(("noop".to_owned(), CID.to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap(),
            before
        );
        assert!(
            remote_pin::Entity::find_by_id(("fast".to_owned(), CID.to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Provider.eq("fast"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            0
        );
        let usage = pin_provider_usage::Entity::find_by_id("noop")
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (usage.reserved_bytes, usage.reserved_pins),
            if current_job { (100, 1) } else { (200, 2) }
        );
        assert_eq!(
            pin_lease_target::Entity::find_by_id(SOURCE_TARGET_ID)
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "quota_waiting"
        );
    }
}

#[tokio::test]
async fn quota_waiter_reuses_shared_suffix_cid_without_duplicate_usage_or_submit() {
    let mut fixture = fixture_with_noop_limits([], 100, 1).await;
    let now = seed_one_quota_waiter(&mut fixture).await;
    assert_eq!(
        store_quota::reserve_unique(
            fixture.store.db(),
            "fast",
            CID,
            100,
            fixture.coordinator.provider_limits(),
            now,
        )
        .await
        .unwrap(),
        store_quota::ReservationOutcome::Reserved
    );
    let shared_created = now - chrono::Duration::seconds(1);
    fixture
        .store
        .db()
        .execute_unprepared(&format!(
            "INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('object-secondary','bucket','secondary','{CID}',100,'{CID}'); \
             UPDATE remote_pins SET request_id='shared-fast',status='pinned' \
             WHERE provider='fast' AND cid='{CID}'; \
             INSERT INTO pin_leases \
             (id,owner_object_id,source,policy_id,provider_mode,content_mode,created_at, \
              last_touched_at,expires_at,generation,state) \
             VALUES ('lease-shared-fast','object-secondary','automatic','policy','all','full','{}','{}','{}',1,'active'); \
             INSERT INTO pin_lease_targets \
             (id,lease_id,cid,logical_size,provider,state,created_at,last_touched_at) \
             VALUES ('target-shared-fast','lease-shared-fast','{CID}',100,'fast','pinned','{}','{}')",
            shared_created.to_rfc3339(),
            shared_created.to_rfc3339(),
            (now + chrono::Duration::hours(1)).to_rfc3339(),
            shared_created.to_rfc3339(),
            shared_created.to_rfc3339(),
        ))
        .await
        .unwrap();

    super::super::quota_waiter::coordinate(&fixture.coordinator, &fixture.store, now)
        .await
        .unwrap();

    let usage = pin_provider_usage::Entity::find_by_id("fast")
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));
    assert_eq!(
        remote_pin::Entity::find()
            .filter(remote_pin::Column::Provider.eq("fast"))
            .filter(remote_pin::Column::Cid.eq(CID))
            .count(fixture.store.db())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq(LEASE_ID))
            .filter(pin_lease_target::Column::Provider.eq("fast"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "pinned"
    );
    assert_eq!(
        pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("fast"))
            .filter(pin_job::Column::Operation.eq("submit"))
            .count(fixture.store.db())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn stateful_keyset_terminates_bad_source_and_advances_independent_provider() {
    let mut fixture = fixture_with_noop_limits([], 100, 1).await;
    PinningCoordinator::configure_worker_for_test(&mut fixture.coordinator, |settings| {
        settings.claim_limit = 1;
    });
    let now = seed_one_quota_waiter(&mut fixture).await;
    let blocked_at = now - chrono::Duration::seconds(1);
    fixture
        .store
        .db()
        .execute_unprepared(&format!(
            "INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('object-secondary','bucket','one/secondary','{CID}',100,'{CID}'); \
             INSERT INTO pin_leases \
             (id,owner_object_id,source,policy_id,provider_mode,content_mode,created_at, \
              last_touched_at,expires_at,generation,state) \
             VALUES ('lease-blocked-prefix','object-secondary','automatic','policy','one','full','{}','{}','{}',1,'active'); \
             INSERT INTO pin_lease_targets \
             (id,lease_id,cid,logical_size,provider,state,created_at,last_touched_at) \
             VALUES ('target-blocked-prefix','lease-blocked-prefix','bafy-blocked-prefix',100, \
                     'third','quota_waiting','{}','{}'), \
                    ('target-blocked-same-provider-tail','lease-blocked-prefix','bafy-blocked-tail',100, \
                     'third','quota_waiting','{}','{}')",
            blocked_at.to_rfc3339(),
            blocked_at.to_rfc3339(),
            (now + chrono::Duration::hours(1)).to_rfc3339(),
            blocked_at.to_rfc3339(),
            blocked_at.to_rfc3339(),
            now.to_rfc3339(),
            now.to_rfc3339(),
        ))
        .await
        .unwrap();
    let mut scan = super::super::quota_waiter::ScanState::default();

    super::super::quota_waiter::coordinate_with_state(
        &fixture.coordinator,
        &fixture.store,
        now,
        &mut scan,
    )
    .await
    .unwrap();
    let failed = pin_lease_target::Entity::find_by_id("target-blocked-prefix")
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.state, "error");
    assert!(failed.last_error.is_some());
    assert_eq!(
        pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("third"))
            .count(fixture.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        pin_lease_target::Entity::find_by_id(SOURCE_TARGET_ID)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "quota_waiting"
    );

    super::super::quota_waiter::coordinate_with_state(
        &fixture.coordinator,
        &fixture.store,
        now,
        &mut scan,
    )
    .await
    .unwrap();
    assert_eq!(
        pin_lease_target::Entity::find_by_id("target-blocked-same-provider-tail")
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "quota_waiting",
        "the blocked provider tail must not bypass its queue head"
    );

    super::super::quota_waiter::coordinate_with_state(
        &fixture.coordinator,
        &fixture.store,
        now,
        &mut scan,
    )
    .await
    .unwrap();
    assert_eq!(
        pin_lease_target::Entity::find_by_id(SOURCE_TARGET_ID)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "released",
        "the persisted keyset must reach a later provider on a bounded future tick"
    );
}

#[tokio::test]
async fn quota_waiter_executes_under_current_managed_route_without_gaining_cleanup_authority() {
    let raw = PinningConfig {
        providers: ["noop", "fast"]
            .into_iter()
            .enumerate()
            .map(|(index, name)| ProviderConfig {
                name: name.to_owned(),
                kind: "noop".into(),
                token_env: None,
                endpoint: None,
                api: None,
                strategy: None,
                upload_endpoint: None,
                cluster: None,
                enabled: true,
                priority: index as u32 + 1,
                max_bytes: 100,
                max_pins: 1,
                requests_per_second: None,
            })
            .collect(),
        ..Default::default()
    };
    let mut config = ValidatedPinningConfig::from_raw(&raw, |_| None).unwrap();
    for provider in &mut config.providers {
        provider.identity.provider_id = format!("quota-{}-provider", provider.name);
        provider.identity.scope = format!("quota-{}", provider.name);
        provider.identity.storage_domain = format!("quota-{}", provider.name);
        if provider.name == "fast" {
            provider.identity.cleanup = CleanupMode::Retain;
        }
    }
    let source = config
        .providers
        .iter()
        .find(|provider| provider.name == "noop")
        .unwrap()
        .identity
        .allocation_key();
    let fast = config
        .providers
        .iter()
        .find(|provider| provider.name == "fast")
        .unwrap()
        .identity
        .allocation_key();
    let mut coordinator = PinningCoordinator::build(config).unwrap();
    let provider = ScriptProvider::new([]);
    PinningCoordinator::replace_provider_for_test(&mut coordinator, &source, provider.clone());
    let db = crate::store::connect_database("sqlite::memory:")
        .await
        .unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    crate::store::bucket::create(&db, "bucket", None)
        .await
        .unwrap();
    let mut fixture = Fixture {
        store: Store::new(db),
        coordinator,
        provider,
        _database_directory: None,
    };
    fixture
        .coordinator
        .register_identities(&fixture.store)
        .await
        .unwrap();
    assert_eq!(
        store_quota::reserve_unique(
            fixture.store.db(),
            &source,
            "bafy-occupied-cleanup",
            100,
            fixture.coordinator.provider_limits(),
            Utc::now()
        )
        .await
        .unwrap(),
        store_quota::ReservationOutcome::Reserved
    );
    let now = seed_one_quota_waiter(&mut fixture).await;
    let registered = pin_provider_route::Entity::find_by_id(&fast)
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    let original: ProviderRouteSnapshot = serde_json::from_str(&registered.snapshot).unwrap();
    assert_eq!(original.cleanup, CleanupMode::Retain);
    let mut config = fixture.coordinator.effective_config().clone();
    config
        .providers
        .iter_mut()
        .find(|provider| provider.name == fast)
        .unwrap()
        .identity
        .cleanup = CleanupMode::Managed;
    fixture.coordinator = PinningCoordinator::build(config).unwrap();
    ledger::register_route(
        fixture.store.db(),
        &fast,
        fixture.coordinator.provider_identity(&fast).unwrap(),
    )
    .await
    .unwrap();
    let registered = pin_provider_route::Entity::find_by_id(&fast)
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    let current: ProviderRouteSnapshot = serde_json::from_str(&registered.snapshot).unwrap();

    let lease = pin_lease::Entity::find_by_id(LEASE_ID)
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    let candidates = super::super::current_target_failover(
        &fixture.store,
        &fixture.coordinator,
        &lease,
        CID,
        &source,
        true,
    )
    .await
    .unwrap();
    assert_eq!(
        candidates.as_deref(),
        Some([source.clone(), fast.clone()].as_slice())
    );
    let work = SharedDecodeBudget::default();
    let tx = fixture
        .store
        .begin_runtime(DecodeWork::shared(&work))
        .await
        .unwrap();
    let replacement = leases::fail_runtime_one_quota_waiter(
        &tx,
        LEASE_ID,
        SOURCE_TARGET_ID,
        lease.generation,
        candidates.as_ref().unwrap(),
        fixture.coordinator.provider_limits(),
        now,
        DecodeWork::shared(&work),
    )
    .await
    .unwrap();
    assert!(
        replacement.is_some(),
        "the free current suffix must reserve the historical cleanup scope"
    );
    tx.commit().await.unwrap();

    let ledger = remote_pin_ledger::Entity::find_by_id((fast.clone(), CID.to_owned()))
        .one(fixture.store.db())
        .await
        .unwrap()
        .expect("the destination reservation must capture route authority");
    let captured: ProviderRouteSnapshot =
        serde_json::from_str(ledger.route.as_deref().unwrap()).unwrap();
    assert_eq!(captured.cleanup, CleanupMode::Retain);
    assert_eq!(current.cleanup, CleanupMode::Managed);
    assert_eq!(
        pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq(&fast))
            .filter(pin_job::Column::Operation.eq("unpin"))
            .count(fixture.store.db())
            .await
            .unwrap(),
        0,
        "waiter admission must not acquire deletion authority"
    );
}

#[tokio::test]
async fn failed_page_head_is_retried_before_same_source_tail_on_the_next_tick() {
    let mut fixture = fixture_with_noop_limits([], 100, 1).await;
    let now = seed_one_quota_waiter(&mut fixture).await;
    let tail_created = now + chrono::Duration::nanoseconds(1);
    fixture
        .store
        .db()
        .execute_unprepared(&format!(
            "INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('object-secondary','bucket','one/secondary','{CID}',100,'{CID}'); \
             INSERT INTO pin_leases \
             (id,owner_object_id,source,policy_id,provider_mode,content_mode,created_at, \
              last_touched_at,expires_at,generation,state) \
             VALUES ('lease-page-tail','object-secondary','automatic','policy','one','full','{}','{}','{}',1,'active'); \
             INSERT INTO pin_lease_targets \
             (id,lease_id,cid,logical_size,provider,state,created_at,last_touched_at) \
             VALUES ('target-page-tail','lease-page-tail','bafy-page-tail',100,'noop', \
                     'quota_waiting','{}','{}')",
            tail_created.to_rfc3339(),
            now.to_rfc3339(),
            (now + chrono::Duration::hours(1)).to_rfc3339(),
            tail_created.to_rfc3339(),
            now.to_rfc3339(),
        ))
        .await
        .unwrap();
    let mut scan = super::super::quota_waiter::ScanState::default();

    pin_lease_target::Entity::update_many()
        .col_expr(
            pin_lease_target::Column::Id,
            Expr::value("target-page-head"),
        )
        .filter(pin_lease_target::Column::Id.eq(SOURCE_TARGET_ID))
        .exec(fixture.store.db())
        .await
        .unwrap();
    for tick in 1..=2 {
        super::super::quota_waiter::fail_candidate_once("target-page-head").await;
        assert!(
            super::super::quota_waiter::coordinate_with_state(
                &fixture.coordinator,
                &fixture.store,
                now,
                &mut scan,
            )
            .await
            .is_err(),
            "tick {tick} must retry the unresolved page head"
        );
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-page-tail")
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "quota_waiting",
            "an errored head must not publish a cursor past its same-source tail"
        );
        assert!(
            pin_provider_usage::Entity::find_by_id("fast")
                .one(fixture.store.db())
                .await
                .unwrap()
                .is_none()
        );
    }
}
