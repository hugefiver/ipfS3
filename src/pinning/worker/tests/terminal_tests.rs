use super::*;
use sea_orm::sea_query::Expr;

#[tokio::test]
async fn terminal_policy_mismatch_survives_ensure_and_other_providers_progress() {
    let mut fixture = fixture([Script::Get(Ok(remote_for(
        "bafy-healthy",
        "request-healthy",
        RemotePinStatus::Pinned,
    )))])
    .await;
    let fast = ScriptProvider::new([Script::Get(Ok(remote_for(
        "bafy-independent",
        "request-independent",
        RemotePinStatus::Pinned,
    )))]);
    PinningCoordinator::replace_provider_for_test(&mut fixture.coordinator, "fast", fast.clone());
    fixture.enqueue_submit().await;
    pin_lease::Entity::update_many()
        .col_expr(pin_lease::Column::Source, Expr::value("invalid"))
        .filter(pin_lease::Column::Id.eq("lease-1"))
        .exec(fixture.store.db())
        .await
        .unwrap();
    fixture.run_one_due().await;
    let job = pin_job::Entity::find()
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.state, "error");
    assert!(job.locked_until.is_none());
    assert!(job.last_error.is_some());
    let target = pin_lease_target::Entity::find_by_id("target-1")
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(target.state, "error");
    assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 0);
    assert!(matches!(
        jobs::resolve_no_request_submit_ambiguity(
            fixture.store.db(),
            "noop",
            "bafy-worker",
            Utc::now(),
        )
        .await
        .unwrap(),
        jobs::NoRequestSubmitAmbiguity::Clear { .. }
    ));
    pin_lease::Entity::update_many()
        .col_expr(pin_lease::Column::Source, Expr::value("automatic"))
        .filter(pin_lease::Column::Id.eq("lease-1"))
        .exec(fixture.store.db())
        .await
        .unwrap();
    let restarted_store = Store::new(fixture.store.db().clone());
    let restarted_coordinator =
        PinningCoordinator::build(fixture.coordinator.effective_config().clone()).unwrap();
    super::super::recover_parked_receipts(
        &restarted_coordinator,
        &restarted_store,
        &mut String::new(),
    )
    .await
    .unwrap();
    super::super::recover_missing_receipt_parks(
        &restarted_coordinator,
        &restarted_store,
        &mut String::new(),
    )
    .await
    .unwrap();
    let jobs::NewPinJob::Target(same) =
        jobs::submit_job("noop", "bafy-worker", "lease-1", "target-1", 1, Utc::now())
    else {
        unreachable!()
    };
    assert_eq!(
        jobs::ensure_or_reactivate_submit_job(fixture.store.db(), same, Utc::now())
            .await
            .unwrap(),
        jobs::EnsureJobOutcome::Terminal
    );
    let jobs::NewPinJob::Target(new_generation) =
        jobs::submit_job("noop", "bafy-worker", "lease-1", "target-1", 2, Utc::now())
    else {
        unreachable!()
    };
    assert_eq!(
        jobs::ensure_or_reactivate_submit_job(fixture.store.db(), new_generation, Utc::now())
            .await
            .unwrap(),
        jobs::EnsureJobOutcome::Terminal
    );
    for _ in 0..2 {
        assert!(
            jobs::claim_due_jobs(
                fixture.store.db(),
                Utc::now() + chrono::Duration::hours(1),
                chrono::Duration::seconds(30),
                8
            )
            .await
            .unwrap()
            .is_empty()
        );
    }
    fixture.seed_poll("healthy", Utc::now()).await;
    fixture
        .seed_poll_for("fast", "independent", Utc::now())
        .await;
    fixture.run_one_due().await;
    fixture.run_one_due().await;
    assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 1);
    assert_eq!(fast.gets.load(Ordering::SeqCst), 1);
    assert_eq!(
        pin_job::Entity::find_by_id(job.id)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "error"
    );
}

#[tokio::test]
async fn old_generation_error_does_not_block_current_owner_submit() {
    let fixture = fixture([Script::Submit(Ok(remote(
        "current-owner",
        RemotePinStatus::Pinned,
    )))])
    .await;
    fixture.enqueue_submit().await;
    let old = jobs::claim_due_jobs(
        fixture.store.db(),
        Utc::now(),
        chrono::Duration::seconds(30),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    pin_lease::Entity::update_many()
        .col_expr(pin_lease::Column::Generation, Expr::value(2))
        .filter(pin_lease::Column::Id.eq("lease-1"))
        .exec(fixture.store.db())
        .await
        .unwrap();
    let tx = fixture.store.db().begin().await.unwrap();
    assert!(
        jobs::terminate_claim(&tx, &old, "stale lease generation", Utc::now())
            .await
            .unwrap()
    );
    tx.commit().await.unwrap();
    assert_eq!(
        pin_lease_target::Entity::find_by_id("target-1")
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "waiting"
    );
    let jobs::NewPinJob::Target(current) =
        jobs::submit_job("noop", "bafy-worker", "lease-1", "target-1", 2, Utc::now())
    else {
        unreachable!()
    };
    assert_eq!(
        jobs::ensure_or_reactivate_submit_job(fixture.store.db(), current, Utc::now())
            .await
            .unwrap(),
        jobs::EnsureJobOutcome::Inserted
    );
    fixture.run_one_due().await;
    assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 1);
    assert_eq!(
        pin_lease_target::Entity::find_by_id("target-1")
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "pinned"
    );
    assert_eq!(
        pin_job::Entity::find_by_id(old.model.id)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "error"
    );
}

#[tokio::test]
async fn invalid_claimed_operation_scope_errors_job_without_poisoning_target() {
    let fixture = fixture([]).await;
    fixture.enqueue_submit().await;
    let mut claimed = jobs::claim_due_jobs(
        fixture.store.db(),
        Utc::now(),
        chrono::Duration::seconds(30),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    let id = claimed.model.id.clone();
    claimed.model.expected_remote_epoch = Some(1);
    super::super::execute_claimed_job(
        &fixture.store,
        &fixture.coordinator,
        &Arc::new(Semaphore::new(2)),
        claimed,
    )
    .await
    .unwrap();
    let job = pin_job::Entity::find_by_id(id)
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.state, "error");
    assert!(job.expected_remote_epoch.is_none());
    assert!(job.locked_until.is_none());
    assert_eq!(
        pin_lease_target::Entity::find_by_id("target-1")
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "waiting"
    );
    assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn old_request_poll_error_does_not_block_same_generation_current_request() {
    let fixture = fixture([Script::Get(Ok(remote(
        "request-new",
        RemotePinStatus::Pinned,
    )))])
    .await;
    fixture.enqueue_poll("request-old").await;
    let old = jobs::claim_due_jobs(
        fixture.store.db(),
        Utc::now(),
        chrono::Duration::seconds(30),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    remote_pin::Entity::update_many()
        .col_expr(remote_pin::Column::RequestId, Expr::value("request-new"))
        .filter(remote_pin::Column::Provider.eq("noop"))
        .filter(remote_pin::Column::Cid.eq("bafy-worker"))
        .exec(fixture.store.db())
        .await
        .unwrap();
    let tx = fixture.store.db().begin().await.unwrap();
    assert!(
        jobs::terminate_claim(&tx, &old, "replaced poll request", Utc::now())
            .await
            .unwrap()
    );
    tx.commit().await.unwrap();
    assert_eq!(
        pin_lease_target::Entity::find_by_id("target-1")
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "submitted"
    );
    let now = Utc::now();
    let jobs::NewPinJob::Target(old_work) = jobs::poll_job(
        "noop",
        "bafy-worker",
        "lease-1",
        "target-1",
        1,
        "request-old",
        now + chrono::Duration::seconds(5),
    ) else {
        unreachable!()
    };
    assert_eq!(
        jobs::ensure_or_reactivate_poll_job(fixture.store.db(), old_work, now)
            .await
            .unwrap(),
        jobs::EnsureJobOutcome::Terminal
    );
    let jobs::NewPinJob::Target(current) = jobs::poll_job(
        "noop",
        "bafy-worker",
        "lease-1",
        "target-1",
        1,
        "request-new",
        now + chrono::Duration::seconds(5),
    ) else {
        unreachable!()
    };
    assert_eq!(
        jobs::ensure_or_reactivate_poll_job(fixture.store.db(), current, now)
            .await
            .unwrap(),
        jobs::EnsureJobOutcome::Inserted
    );
    fixture.run_one_due().await;
    assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 0);
    assert_eq!(
        pin_lease_target::Entity::find_by_id("target-1")
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "pinned"
    );
    assert_eq!(
        pin_lease::Entity::find_by_id("lease-1")
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .generation,
        1
    );
    assert_eq!(
        pin_job::Entity::find_by_id(old.model.id)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "error"
    );
}

#[tokio::test]
async fn active_error_shared_reference_blocks_queued_unpin_until_lease_ends() {
    use crate::store::pinning::{
        ledger,
        management::records::{DecodeWork, SharedDecodeBudget},
        quota,
    };
    for expire in [false, true] {
        let fixture = fixture([
            Script::Submit(Ok(remote("owned-shared", RemotePinStatus::Queued))),
            Script::Unpin(Ok(())),
        ])
        .await;
        ledger::register_route(
            fixture.store.db(),
            "noop",
            fixture.coordinator.provider_identity("noop").unwrap(),
        )
        .await
        .unwrap();
        ledger::capture_reservation(fixture.store.db(), "noop", "bafy-worker")
            .await
            .unwrap();
        fixture.enqueue_submit().await;
        fixture.run_one_due().await;
        ledger::observe(
            fixture.store.db(),
            "noop",
            "bafy-worker",
            RemotePinStatus::Queued,
            ledger::Ownership::ApplicationCreated,
            Utc::now(),
        )
        .await
        .unwrap();
        assert!(
            ledger::cleanup_allowed(fixture.store.db(), "noop", "bafy-worker")
                .await
                .unwrap()
        );
        let now = Utc::now().to_rfc3339();
        let expires = (Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
        fixture.store.db().execute_unprepared(&format!(
            "INSERT INTO objects(id,bucket,key,cid,size,etag) VALUES('object-shared','bucket','shared','bafy-worker',100,'bafy-worker');
             INSERT INTO pin_leases(id,owner_object_id,source,policy_id,provider_mode,content_mode,created_at,last_touched_at,expires_at,generation,state)
             VALUES('lease-shared','object-shared','automatic','policy','all','full','{now}','{now}','{expires}',1,'active');
             INSERT INTO pin_lease_targets(id,lease_id,cid,logical_size,provider,state,created_at,last_touched_at)
             VALUES('target-shared','lease-shared','bafy-worker',100,'noop','submitted','{now}','{now}');
             UPDATE pin_leases SET source='invalid' WHERE id='lease-1'"
        )).await.unwrap();
        fixture.run_one_due().await;
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-1")
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "error"
        );
        let tx = fixture.store.db().begin().await.unwrap();
        leases::cancel_lease(&tx, "lease-shared", Utc::now())
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(
            !ledger::cleanup_allowed(fixture.store.db(), "noop", "bafy-worker")
                .await
                .unwrap()
        );
        let resource =
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap();
        let budget = SharedDecodeBudget::default();
        let work = DecodeWork::shared(&budget);
        let tx = fixture.store.begin_runtime(work).await.unwrap();
        assert_eq!(
            quota::confirmed_release_for_request(
                tx.db(),
                "noop",
                "bafy-worker",
                resource.epoch,
                resource.request_id.as_deref(),
                Utc::now(),
            )
            .await
            .unwrap(),
            quota::ConfirmedReleaseOutcome::Stale
        );
        tx.commit().await.unwrap();
        jobs::enqueue_job(
            fixture.store.db(),
            jobs::unpin_job("noop", "bafy-worker", resource.epoch, Utc::now()),
        )
        .await
        .unwrap();
        fixture.run_one_due().await;
        assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 0);
        let held = pin_provider_usage::Entity::find_by_id("noop")
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((held.reserved_bytes, held.reserved_pins), (100, 1));
        assert_eq!(
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .request_id,
            resource.request_id
        );
        let tx = fixture.store.db().begin().await.unwrap();
        if expire {
            pin_lease::Entity::update_many()
                .col_expr(
                    pin_lease::Column::ExpiresAt,
                    Expr::value(Utc::now() - chrono::Duration::seconds(1)),
                )
                .filter(pin_lease::Column::Id.eq("lease-1"))
                .exec(&tx)
                .await
                .unwrap();
            assert_eq!(
                leases::expire_due_leases(&tx, Utc::now()).await.unwrap(),
                vec!["lease-1"]
            );
        } else {
            leases::cancel_lease(&tx, "lease-1", Utc::now())
                .await
                .unwrap();
        }
        tx.commit().await.unwrap();
        assert!(
            ledger::cleanup_allowed(fixture.store.db(), "noop", "bafy-worker")
                .await
                .unwrap()
        );
        fixture.run_one_due().await;
        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 1);
        let released = pin_provider_usage::Entity::find_by_id("noop")
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((released.reserved_bytes, released.reserved_pins), (0, 0));
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-1")
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "error"
        );
    }
}

#[tokio::test]
async fn active_error_no_request_hold_uses_persistent_tables_and_releases_after_cancel() {
    use crate::store::pinning::{
        management::records::{DecodeWork, SharedDecodeBudget},
        quota,
    };
    let fixture = fixture([]).await;
    fixture.enqueue_submit().await;
    pin_lease::Entity::update_many()
        .col_expr(pin_lease::Column::Source, Expr::value("invalid"))
        .filter(pin_lease::Column::Id.eq("lease-1"))
        .exec(fixture.store.db())
        .await
        .unwrap();
    fixture.run_one_due().await;
    assert!(matches!(
        jobs::resolve_no_request_submit_ambiguity(
            fixture.store.db(),
            "noop",
            "bafy-worker",
            Utc::now()
        )
        .await
        .unwrap(),
        jobs::NoRequestSubmitAmbiguity::Clear { .. }
    ));
    let tx = fixture.store.db().begin().await.unwrap();
    tx.execute_unprepared("CREATE TEMP TABLE pin_leases AS SELECT * FROM main.pin_leases WHERE 0; CREATE TEMP TABLE pin_lease_targets AS SELECT * FROM main.pin_lease_targets WHERE 0").await.unwrap();
    let budget = SharedDecodeBudget::default();
    assert!(
        leases::has_active_error_hold(&tx, "noop", "bafy-worker", DecodeWork::shared(&budget))
            .await
            .unwrap()
    );
    assert!(
        !leases::has_active_error_hold(&tx, "noop", "other-cid", DecodeWork::shared(&budget))
            .await
            .unwrap()
    );
    assert_eq!(
        quota::confirmed_release(&tx, "noop", "bafy-worker", 1, Utc::now())
            .await
            .unwrap(),
        quota::ConfirmedReleaseOutcome::Stale
    );
    assert_eq!(
        leases::complete_no_request_remote_absence(&tx, "noop", "bafy-worker", 1, Utc::now())
            .await
            .unwrap(),
        leases::NoRequestRemoteCompletion::Stale
    );
    tx.execute_unprepared("DROP TABLE temp.pin_lease_targets; DROP TABLE temp.pin_leases")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let held = pin_provider_usage::Entity::find_by_id("noop")
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((held.reserved_bytes, held.reserved_pins), (100, 1));
    let jobs::NewPinJob::Remote(reconcile) =
        jobs::reconcile_job("noop", "bafy-worker", 1, Utc::now())
    else {
        unreachable!()
    };
    let reconcile_id = reconcile.id.clone();
    jobs::enqueue_job(fixture.store.db(), jobs::NewPinJob::Remote(reconcile))
        .await
        .unwrap();
    fixture.run_one_due().await;
    assert_eq!(
        pin_job::Entity::find_by_id(&reconcile_id)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "error"
    );
    assert!(
        jobs::claim_due_jobs(
            fixture.store.db(),
            Utc::now() + chrono::Duration::minutes(1),
            chrono::Duration::seconds(30),
            8
        )
        .await
        .unwrap()
        .is_empty()
    );
    let held = pin_provider_usage::Entity::find_by_id("noop")
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((held.reserved_bytes, held.reserved_pins), (100, 1));
    let tx = fixture.store.db().begin().await.unwrap();
    leases::cancel_lease(&tx, "lease-1", Utc::now())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        pin_lease::Entity::find_by_id("lease-1")
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "cancelled"
    );
    fixture.run_one_due().await;
    fixture.run_one_due().await;
    let released = pin_provider_usage::Entity::find_by_id("noop")
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((released.reserved_bytes, released.reserved_pins), (0, 0));
    assert_eq!(
        pin_job::Entity::find_by_id(reconcile_id)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "error"
    );
    assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn decompressed_revocation_terminates_without_changing_expiry() {
    let fixture = fixture([]).await;
    fixture.enqueue_submit().await;
    pin_lease::Entity::update_many()
        .col_expr(pin_lease::Column::ContentMode, Expr::value("decompressed"))
        .filter(pin_lease::Column::Id.eq("lease-1"))
        .exec(fixture.store.db())
        .await
        .unwrap();
    let before = pin_lease::Entity::find_by_id("lease-1")
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    fixture.run_one_due().await;
    let after = pin_lease::Entity::find_by_id("lease-1")
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.expires_at, before.expires_at);
    assert_eq!(
        pin_job::Entity::find()
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "error"
    );
    assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn stale_termination_does_not_touch_reclaimed_owner() {
    let fixture = fixture([]).await;
    fixture.enqueue_submit().await;
    let now = Utc::now();
    let stale = jobs::claim_due_jobs(fixture.store.db(), now, chrono::Duration::seconds(1), 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let fresh = jobs::claim_due_jobs(
        fixture.store.db(),
        now + chrono::Duration::seconds(2),
        chrono::Duration::seconds(30),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    let tx = fixture.store.db().begin().await.unwrap();
    assert!(
        !jobs::terminate_claim(&tx, &stale, "current authority mismatch", now)
            .await
            .unwrap()
    );
    tx.commit().await.unwrap();
    let current = pin_job::Entity::find_by_id(&fresh.model.id)
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.state, "running");
    assert_eq!(current.locked_until, fresh.model.locked_until);
    assert_eq!(
        pin_lease_target::Entity::find_by_id("target-1")
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "waiting"
    );
}
