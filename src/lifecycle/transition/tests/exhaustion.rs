use super::*;

const ATTEMPTS_EXHAUSTED: &str = "transition_attempts_exhausted";

#[tokio::test]
async fn valid_transition_without_tiers_remains_retryable_before_exhaustion() {
    let fixture = fixture(Selector::All).await;
    crate::lifecycle::worker::execute_claimed_action(
        fixture.db(),
        &fixture.claim,
        &worker_config(),
        None,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let action = stored_action(&fixture).await;
    assert_eq!(action.state, "pending");
    assert_eq!(action.failure_class.as_deref(), Some("internal_dependency"));
    assert!(action.finished_at.is_none());
    assert_eq!(
        residency(&fixture, &fixture.version.id).await.storage_class,
        StorageClass::Standard
    );
}

fn max_attempts_one() -> ValidatedLifecycleConfig {
    let mut config = worker_config();
    config.max_attempts = 1;
    config
}

async fn transition_hold_count(fixture: &Fixture, saga_id: &str) -> u64 {
    residency_reference::Entity::find()
        .filter(residency_reference::Column::OwnerKind.eq("transition"))
        .filter(residency_reference::Column::OwnerId.eq(saga_id))
        .count(fixture.db())
        .await
        .unwrap()
}

async fn assert_exhausted_cancellation(fixture: &Fixture, failure_class: &str) {
    let action = stored_action(fixture).await;
    assert_eq!(action.state, "cancelled");
    assert_eq!(action.failure_class.as_deref(), Some(failure_class));
    assert_eq!(
        action.last_error_redacted.as_deref(),
        Some(ATTEMPTS_EXHAUSTED)
    );
    assert!(action.finished_at.is_some());
    assert!(action.lease_until.is_none());
    assert!(action.claimed_by.is_none());

    let saga = stored_saga(fixture).await;
    assert_eq!(saga.settlement_kind.as_deref(), Some("cancelled"));
    assert!(saga.completed_at.is_some());
    assert!(saga.publication_receipt.is_none());
    assert_eq!(transition_hold_count(fixture, &saga.id).await, 0);
}

async fn assert_no_pin_removal(harness: &TierHarness) {
    for server in [&harness.hot_server, &harness.cold_server] {
        assert!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.url.path() != "/api/v0/pin/rm")
        );
    }
}

async fn delete_exact_version(fixture: &Fixture, key: &str, version: &object_version::Model) {
    let now = store::database_clock::database_now(fixture.db())
        .await
        .unwrap();
    let guard = admit_content_mutation(
        fixture.db(),
        BUCKET,
        key,
        None,
        SupersedeReason::DeleteObject,
        now,
    )
    .await
    .unwrap();
    let public_version = PublicVersionId::parse_s3(
        version
            .version_id
            .as_deref()
            .expect("enabled fixture version has a public ID"),
    )
    .unwrap();
    delete_version_with_leases_guarded(
        fixture.db(),
        BUCKET,
        key,
        VersionSelector::Exact(public_version),
        guard,
        now,
    )
    .await
    .unwrap();
}

async fn expire_and_claim_at_cap(fixture: &Fixture, worker: &str) -> ClaimedLifecycleAction {
    let past = store::database_clock::database_now(fixture.db())
        .await
        .unwrap()
        - Duration::seconds(1);
    let updated = lifecycle_action::Entity::update_many()
        .col_expr(
            lifecycle_action::Column::LeaseUntil,
            Expr::value(Some(past)),
        )
        .col_expr(lifecycle_action::Column::NextAttemptAt, Expr::value(past))
        .filter(lifecycle_action::Column::Id.eq(&fixture.claim.action.id))
        .filter(lifecycle_action::Column::State.eq("claimed"))
        .filter(lifecycle_action::Column::ClaimEpoch.eq(fixture.claim.claim_epoch))
        .exec(fixture.db())
        .await
        .unwrap();
    assert_eq!(updated.rows_affected, 1);
    let mut claims = store::lifecycle_action::claim_due_with_max_attempts(
        fixture.db(),
        worker,
        Duration::seconds(30),
        1,
        1,
    )
    .await
    .unwrap();
    assert_eq!(claims.len(), 1);
    claims.pop().unwrap()
}

#[tokio::test]
async fn final_import_failure_settles_and_unblocks_version_and_bucket_deletion() {
    let fixture = fixture(Selector::All).await;
    store::bucket::create(fixture.db(), "retained-bucket", None)
        .await
        .unwrap();
    let mut shared_request = publication_request(
        "shared-object",
        "shared",
        CID,
        Vec::new(),
        vec![LeaseIntent {
            source: LeaseSource::Manual,
            policy_id: "policy:manual-fixture".to_owned(),
            provider_mode: ProviderMode::One,
            providers: vec!["manual-provider".to_owned()],
            content_mode: ContentMode::Object,
            duration: LeaseDuration::parse("1h").unwrap(),
        }],
    );
    shared_request.object.bucket = "retained-bucket".to_owned();
    let shared = publish_request(fixture.db(), shared_request, &manual_limits()).await;
    verify_hot(fixture.db(), &shared).await;
    let lease_before = pin_lease::Entity::find()
        .filter(pin_lease::Column::OwnerObjectId.eq("shared-object"))
        .filter(pin_lease::Column::Source.eq("manual"))
        .one(fixture.db())
        .await
        .unwrap()
        .unwrap();
    let targets_before = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(&lease_before.id))
        .all(fixture.db())
        .await
        .unwrap();
    let harness = TierHarness::new(ImportBehavior::Failure, HOT_NODE).await;

    execute(
        fixture.db(),
        &fixture.claim,
        &harness.clients(),
        &max_attempts_one(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_exhausted_cancellation(&fixture, "internal_dependency").await;
    assert_eq!(stored_action(&fixture).await.attempts, 1);
    let shared_residency = residency(&fixture, &shared.id).await;
    assert_eq!(shared_residency.primary.tier, KuboTier::Hot);
    assert_eq!(shared_residency.storage_class, StorageClass::Standard);
    assert_eq!(
        pin_lease::Entity::find_by_id(&lease_before.id)
            .one(fixture.db())
            .await
            .unwrap()
            .unwrap(),
        lease_before
    );
    assert_eq!(
        pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq(&lease_before.id))
            .all(fixture.db())
            .await
            .unwrap(),
        targets_before
    );
    assert_no_pin_removal(&harness).await;

    delete_configuration(fixture.db(), BUCKET).await.unwrap();
    delete_exact_version(&fixture, KEY, &fixture.version).await;
    assert_eq!(residency(&fixture, &shared.id).await, shared_residency);
    assert_eq!(
        pin_lease::Entity::find_by_id(&lease_before.id)
            .one(fixture.db())
            .await
            .unwrap()
            .unwrap(),
        lease_before
    );
    assert_eq!(
        pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq(&lease_before.id))
            .all(fixture.db())
            .await
            .unwrap(),
        targets_before
    );
    store::bucket::delete(fixture.db(), BUCKET).await.unwrap();
    assert!(!store::bucket::exists(fixture.db(), BUCKET).await.unwrap());
    assert_eq!(residency(&fixture, &shared.id).await, shared_residency);
    assert_eq!(
        pin_lease::Entity::find_by_id(&lease_before.id)
            .one(fixture.db())
            .await
            .unwrap()
            .unwrap(),
        lease_before
    );
    assert_eq!(
        pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq(&lease_before.id))
            .all(fixture.db())
            .await
            .unwrap(),
        targets_before
    );
    assert_no_pin_removal(&harness).await;
}

#[tokio::test]
async fn final_verification_failure_atomically_settles_the_copy_checkpoint() {
    for verified_checkpoint in [false, true] {
        let fixture = fixture(Selector::All).await;
        let harness = TierHarness::new(ImportBehavior::Success, HOT_NODE).await;
        let guard = admit(&fixture).await;
        let prepared = prepare_checkpoint(&fixture, &guard).await;
        let receipt = copy_receipt(&harness).await;
        let copied = record_copy_checkpoint(&fixture, &prepared).await;
        if verified_checkpoint {
            let txn = fixture.db().begin().await.unwrap();
            record_verified(&txn, &fixture.claim, &copied, &receipt)
                .await
                .unwrap()
                .unwrap();
            txn.commit().await.unwrap();
        }
        let verification_before = stored_saga(&fixture).await.verification_receipt;
        assert!(transition_hold_count(&fixture, &copied.id).await > 0);
        Mock::given(method("POST"))
            .and(path("/api/v0/files/stat"))
            .and(query_param("arg", format!("/ipfs/{CID}")))
            .respond_with(ResponseTemplate::new(503))
            .with_priority(1)
            .mount(&harness.cold_server)
            .await;

        execute(
            fixture.db(),
            &fixture.claim,
            &harness.clients(),
            &max_attempts_one(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_exhausted_cancellation(&fixture, "internal_dependency").await;
        let settled = stored_saga(&fixture).await;
        assert_eq!(
            settled.checkpoint,
            if verified_checkpoint {
                "verify"
            } else {
                "copy"
            }
        );
        assert_eq!(settled.verification_receipt, verification_before);
        assert_no_pin_removal(&harness).await;
    }
}

#[tokio::test]
async fn configuration_delete_during_final_failing_io_wins_as_stale_cancellation() {
    let fixture = fixture(Selector::All).await;
    let harness = TierHarness::new(ImportBehavior::Success, HOT_NODE).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/import"))
        .and(query_param("pin-roots", "true"))
        .and(query_param("stats", "true"))
        .respond_with(ResponseTemplate::new(503).set_delay(StdDuration::from_millis(300)))
        .with_priority(1)
        .mount(&harness.cold_server)
        .await;
    let task = spawn_execute(&fixture, &harness, max_attempts_one());
    wait_for_import(&harness.cold_server).await;

    delete_configuration(fixture.db(), BUCKET).await.unwrap();
    task.await.unwrap().unwrap();

    assert_exhausted_cancellation(&fixture, "cancelled_stale").await;
    assert_no_pin_removal(&harness).await;
}

#[tokio::test]
async fn expired_final_claim_is_reclaimed_for_settlement_and_old_epoch_is_fenced() {
    for copied_checkpoint in [false, true] {
        let mut fixture = fixture(Selector::All).await;
        let harness = TierHarness::new(ImportBehavior::Success, HOT_NODE).await;
        let guard = admit(&fixture).await;
        let prepared = prepare_checkpoint(&fixture, &guard).await;
        if copied_checkpoint {
            copy_receipt(&harness).await;
            record_copy_checkpoint(&fixture, &prepared).await;
        }
        let requests_before = harness.hot_server.received_requests().await.unwrap().len()
            + harness.cold_server.received_requests().await.unwrap().len();
        let old_claim = fixture.claim.clone();
        let replacement = expire_and_claim_at_cap(&fixture, "settlement-worker").await;
        assert_eq!(replacement.claim_epoch, old_claim.claim_epoch + 1);
        assert_eq!(replacement.action.attempts, 2);

        execute(
            fixture.db(),
            &old_claim,
            &harness.clients(),
            &max_attempts_one(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        let fenced = stored_action(&fixture).await;
        assert_eq!(fenced.state, "claimed");
        assert_eq!(fenced.claim_epoch, replacement.claim_epoch);
        assert_eq!(fenced.claimed_by.as_deref(), Some("settlement-worker"));

        fixture.claim = replacement;
        execute(
            fixture.db(),
            &fixture.claim,
            &harness.clients(),
            &max_attempts_one(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_exhausted_cancellation(&fixture, "internal_dependency").await;
        let requests_after = harness.hot_server.received_requests().await.unwrap().len()
            + harness.cold_server.received_requests().await.unwrap().len();
        assert_eq!(
            requests_after, requests_before,
            "settlement replay must not contact Kubo; copied={copied_checkpoint}"
        );
    }
}

#[tokio::test]
async fn legacy_failed_safe_saga_is_marked_and_settled_without_tiers_or_new_io() {
    let fixture = fixture(Selector::All).await;
    let guard = admit(&fixture).await;
    let saga = prepare_checkpoint(&fixture, &guard).await;
    assert_eq!(transition_hold_count(&fixture, &saga.id).await, 1);
    let now = store::database_clock::database_now(fixture.db())
        .await
        .unwrap();
    let stranded = lifecycle_action::Entity::update_many()
        .col_expr(lifecycle_action::Column::State, Expr::value("failed_safe"))
        .col_expr(
            lifecycle_action::Column::LeaseUntil,
            Expr::value(Option::<chrono::DateTime<Utc>>::None),
        )
        .col_expr(
            lifecycle_action::Column::ClaimedBy,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            lifecycle_action::Column::FailureClass,
            Expr::value(Some("internal_dependency".to_owned())),
        )
        .col_expr(
            lifecycle_action::Column::LastErrorRedacted,
            Expr::value(Some("lifecycle action failed".to_owned())),
        )
        .col_expr(lifecycle_action::Column::FinishedAt, Expr::value(Some(now)))
        .col_expr(lifecycle_action::Column::UpdatedAt, Expr::value(now))
        .filter(lifecycle_action::Column::Id.eq(&fixture.claim.action.id))
        .filter(lifecycle_action::Column::State.eq("claimed"))
        .filter(lifecycle_action::Column::ClaimEpoch.eq(fixture.claim.claim_epoch))
        .exec(fixture.db())
        .await
        .unwrap();
    assert_eq!(stranded.rows_affected, 1);

    let mut claims = store::lifecycle_action::claim_due_with_max_attempts(
        fixture.db(),
        "legacy-settlement-worker",
        Duration::seconds(30),
        8,
        1,
    )
    .await
    .unwrap();
    assert_eq!(claims.len(), 1);
    let recovery = claims.pop().unwrap();
    assert_eq!(
        recovery.action.last_error_redacted.as_deref(),
        Some(store::lifecycle_action::TRANSITION_SETTLEMENT_REQUIRED)
    );
    let before_attempts = recovery.action.attempts;

    crate::lifecycle::worker::execute_claimed_action(
        fixture.db(),
        &recovery,
        &worker_config(),
        None,
        &CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_exhausted_cancellation(&fixture, "internal_dependency").await;
    assert_eq!(stored_action(&fixture).await.attempts, before_attempts);
}

#[tokio::test]
async fn terminal_settlement_failure_rolls_back_saga_refs_and_is_reclaimable() {
    let mut fixture = fixture(Selector::All).await;
    let harness = TierHarness::new(ImportBehavior::Failure, HOT_NODE).await;
    fixture
        .db()
        .execute_unprepared(
            "CREATE TRIGGER abort_transition_terminal_cancel \
             BEFORE UPDATE OF state ON lifecycle_actions \
             WHEN NEW.state = 'cancelled' \
             BEGIN SELECT RAISE(ABORT, 'forced terminal cancellation failure'); END",
        )
        .await
        .unwrap();

    let error = execute(
        fixture.db(),
        &fixture.claim,
        &harness.clients(),
        &max_attempts_one(),
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, AppError::Database(_)));
    let action = stored_action(&fixture).await;
    assert_eq!(action.state, "claimed");
    assert!(action.finished_at.is_none());
    let saga = stored_saga(&fixture).await;
    assert!(saga.settlement_kind.is_none());
    assert!(saga.completed_at.is_none());
    assert_eq!(transition_hold_count(&fixture, &saga.id).await, 1);
    let stranded_guard = store::entities::import_destination::Entity::find_by_id((
        BUCKET.to_owned(),
        KEY.to_owned(),
    ))
    .one(fixture.db())
    .await
    .unwrap()
    .unwrap();
    assert!(stranded_guard.mutation_id.is_some());

    fixture
        .db()
        .execute_unprepared("DROP TRIGGER abort_transition_terminal_cancel")
        .await
        .unwrap();
    let requests_before = harness.hot_server.received_requests().await.unwrap().len()
        + harness.cold_server.received_requests().await.unwrap().len();
    fixture.claim = expire_and_claim_at_cap(&fixture, "rollback-recovery-worker").await;
    execute(
        fixture.db(),
        &fixture.claim,
        &harness.clients(),
        &max_attempts_one(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_exhausted_cancellation(&fixture, "internal_dependency").await;
    let released_guard = store::entities::import_destination::Entity::find_by_id((
        BUCKET.to_owned(),
        KEY.to_owned(),
    ))
    .one(fixture.db())
    .await
    .unwrap();
    assert!(released_guard.is_none_or(|guard| guard.mutation_id.is_none()));
    let requests_after = harness.hot_server.received_requests().await.unwrap().len()
        + harness.cold_server.received_requests().await.unwrap().len();
    assert_eq!(requests_after, requests_before);
}
