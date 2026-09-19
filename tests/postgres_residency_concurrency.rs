use std::{sync::Arc, time::Duration};

use chrono::Duration as ChronoDuration;
use ipfs_s3_gateway::{
    config::LifecycleWorkerConfig,
    import::SupersedeReason,
    lifecycle::{
        config::canonical_json,
        model::{
            CanonicalFilter, CanonicalLifecycleConfiguration, CanonicalLifecycleRule,
            LifecycleRuleStatus, NoncurrentExpiration,
        },
        worker::{LifecycleAfterClaimGate, LifecycleWorkerTestControl, start_worker_for_test},
    },
    residency::{
        ClaimedResidencyBackfill, ResidencyBackfillCursor, VerificationState,
        VersionResidencyIdentity,
    },
    store::{
        Store,
        database_clock::database_now,
        entities::{
            lifecycle_action, object_version, physical_residency, pin_lease, pin_lease_target,
            residency_backfill, residency_reference, version_residency,
        },
        import::ownership::admit_content_mutation,
        lifecycle_config::put_configuration,
        object_version::{BucketVersioningState, PublicVersionId, VersionSelector},
        pinning::publication::{delete_version_with_leases_guarded, publish_standard_object},
        residency::{
            checkpoint_residency_backfill_in_transaction, claim_residency_backfill,
            mark_hot_verified_in_transaction, pending_hot_residency_page,
            release_residency_backfill_in_transaction, resolve_version_residency,
        },
    },
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait, PaginatorTrait,
    QueryFilter, Statement, TransactionTrait,
};
use tokio::sync::{Barrier, Notify};
use tokio_util::sync::CancellationToken;

#[path = "support/pg_residency.rs"]
mod pg_residency;

use pg_residency::{
    PgResidencyFixture, backend_pid, create_bucket, guarded_exact_delete, guarded_publish,
    provider_limits, publication_request, publish, version_for_object, wait_until_blocked_by,
};

const OLD_CID: &str = "bafy-pg-backfill-old";
const REPLACEMENT_CID: &str = "bafy-pg-backfill-replacement";
const NODE: &str = "postgres-hot-node";
const RECEIPT: &str = "postgres-controlled-verification-receipt";

#[derive(Clone)]
struct BackfillSnapshot {
    claim: ClaimedResidencyBackfill,
    identity: VersionResidencyIdentity,
    next_cursor: ResidencyBackfillCursor,
    completes_pass: bool,
}

async fn claim_snapshot(db: &DatabaseConnection, worker: &str) -> BackfillSnapshot {
    let claim = claim_residency_backfill(db, worker, ChronoDuration::seconds(30))
        .await
        .expect("claim residency backfill")
        .expect("fixture backfill must be claimable");
    let page = pending_hot_residency_page(db, claim.cursor.as_ref(), 2)
        .await
        .expect("scan pending hot residency page");
    assert_eq!(page.items.len(), 1, "fixture must expose one pending owner");
    let identity = page.items[0].identity.clone();
    BackfillSnapshot {
        claim,
        next_cursor: ResidencyBackfillCursor {
            version_row_id: identity.version_row_id.clone(),
        },
        identity,
        completes_pass: page.complete,
    }
}

async fn finish_after_stale_owner(db: &DatabaseConnection, snapshot: &BackfillSnapshot) {
    let write = db
        .begin()
        .await
        .expect("begin stale verification writeback");
    assert!(
        mark_hot_verified_in_transaction(&write, &snapshot.identity, NODE, RECEIPT)
            .await
            .is_err(),
        "a deleted snapshot owner must reject the verification receipt"
    );
    write
        .rollback()
        .await
        .expect("rollback rejected verification writeback");

    let checkpoint = db.begin().await.expect("begin stale-owner checkpoint");
    assert!(
        checkpoint_residency_backfill_in_transaction(
            &checkpoint,
            &snapshot.claim,
            Some(&snapshot.next_cursor),
            snapshot.completes_pass,
        )
        .await
        .expect("checkpoint stale-owner scan"),
        "owner loss does not invalidate the still-current backfill epoch"
    );
    checkpoint
        .commit()
        .await
        .expect("commit stale-owner checkpoint");
}

async fn assert_completed_backfill(db: &DatabaseConnection) {
    let state = residency_backfill::Entity::find_by_id("hot_verification")
        .one(db)
        .await
        .expect("load residency backfill state")
        .expect("residency backfill state exists");
    assert!(state.completed);
    assert!(state.claimed_by.is_none());
    assert!(state.lease_until.is_none());
}

async fn physical(db: &DatabaseConnection, cid: &str) -> physical_residency::Model {
    physical_residency::Entity::find_by_id(("hot".to_owned(), cid.to_owned()))
        .one(db)
        .await
        .expect("load hot physical residency")
        .expect("hot physical residency exists")
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL (real PostgreSQL)"]
async fn postgres_put_overwrite_wins_during_backfill_io_and_stale_receipt_is_rejected() {
    let fixture = PgResidencyFixture::new().await;
    create_bucket(
        &fixture.db,
        "put-io-winner",
        BucketVersioningState::Suspended,
    )
    .await;
    publish(
        &fixture.db,
        "put-old-object",
        "put-io-winner",
        "key",
        OLD_CID,
        false,
    )
    .await;
    let old_version = version_for_object(&fixture.db, "put-old-object").await;
    let snapshot = claim_snapshot(&fixture.db, "backfill-put-stale").await;
    assert_eq!(snapshot.identity.version_row_id, old_version.id);

    let actor_db = fixture.connection().await;
    let io_arrived = Arc::new(Barrier::new(2));
    let io_resume = Arc::new(Notify::new());
    let actor_barrier = io_arrived.clone();
    let actor_resume = io_resume.clone();
    let actor_snapshot = snapshot.clone();
    let actor = tokio::spawn(async move {
        actor_barrier.wait().await;
        actor_resume.notified().await;
        finish_after_stale_owner(&actor_db, &actor_snapshot).await;
    });

    io_arrived.wait().await;
    guarded_publish(
        &fixture.db,
        "put-replacement-object",
        "put-io-winner",
        "key",
        REPLACEMENT_CID,
    )
    .await;
    io_resume.notify_one();
    actor.await.expect("join stale PUT backfill actor");

    assert!(
        object_version::Entity::find_by_id(&old_version.id)
            .one(&fixture.db)
            .await
            .expect("query displaced version")
            .is_none()
    );
    let replacement = version_for_object(&fixture.db, "put-replacement-object").await;
    let residency = resolve_version_residency(&fixture.db, &replacement.id)
        .await
        .expect("resolve replacement residency");
    assert_eq!(residency.identity.cid, REPLACEMENT_CID);
    assert_eq!(
        residency.physical.verification_state,
        VerificationState::Pending
    );
    assert!(residency.physical.verification_receipt.is_none());
    assert_eq!(
        physical(&fixture.db, OLD_CID).await.verification_state,
        "pending"
    );
    assert_completed_backfill(&fixture.db).await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL (real PostgreSQL)"]
async fn postgres_backfill_writeback_blocks_then_loses_to_put_overwrite_without_receipt_transfer() {
    let fixture = PgResidencyFixture::new().await;
    create_bucket(
        &fixture.db,
        "put-writeback-winner",
        BucketVersioningState::Suspended,
    )
    .await;
    publish(
        &fixture.db,
        "put-locked-old",
        "put-writeback-winner",
        "key",
        OLD_CID,
        false,
    )
    .await;
    let old_version = version_for_object(&fixture.db, "put-locked-old").await;
    let snapshot = claim_snapshot(&fixture.db, "backfill-put-first").await;

    let verifier_db = fixture.connection().await;
    let verifier_pid = backend_pid(&verifier_db).await;
    let transaction = verifier_db
        .begin()
        .await
        .expect("begin verification transaction");
    assert!(
        mark_hot_verified_in_transaction(&transaction, &snapshot.identity, NODE, RECEIPT)
            .await
            .expect("mark old PUT owner verified")
    );

    let contender_db = fixture.connection().await;
    let contender_pid = backend_pid(&contender_db).await;
    let publication_started = Arc::new(Notify::new());
    let actor_started = publication_started.clone();
    let contender = tokio::spawn(async move {
        let now = database_now(&contender_db)
            .await
            .expect("read publication database clock");
        let guard = admit_content_mutation(
            &contender_db,
            "put-writeback-winner",
            "key",
            None,
            SupersedeReason::PutObject,
            now,
        )
        .await
        .expect("admit contending PUT");
        actor_started.notify_one();
        publish_standard_object(
            &contender_db,
            publication_request(
                "put-after-writeback",
                "put-writeback-winner",
                "key",
                REPLACEMENT_CID,
                false,
            ),
            guard,
            &provider_limits(),
        )
        .await
        .expect("finish contending PUT")
    });

    publication_started.notified().await;
    wait_until_blocked_by(&fixture.db, contender_pid, verifier_pid).await;
    assert!(
        object_version::Entity::find_by_id(&old_version.id)
            .one(&fixture.db)
            .await
            .expect("observe locked PUT owner")
            .is_some()
    );
    assert!(
        checkpoint_residency_backfill_in_transaction(
            &transaction,
            &snapshot.claim,
            Some(&snapshot.next_cursor),
            snapshot.completes_pass,
        )
        .await
        .expect("checkpoint writeback-first PUT")
    );
    transaction
        .commit()
        .await
        .expect("commit writeback before PUT");
    contender.await.expect("join blocked PUT actor");

    assert!(
        object_version::Entity::find_by_id(&old_version.id)
            .one(&fixture.db)
            .await
            .expect("query overwritten owner")
            .is_none()
    );
    let replacement = version_for_object(&fixture.db, "put-after-writeback").await;
    let replacement_residency = resolve_version_residency(&fixture.db, &replacement.id)
        .await
        .expect("resolve PUT replacement");
    assert_eq!(
        replacement_residency.physical.verification_state,
        VerificationState::Pending
    );
    assert!(
        replacement_residency
            .physical
            .verification_receipt
            .is_none()
    );
    let old_physical = physical(&fixture.db, OLD_CID).await;
    assert_eq!(old_physical.verification_state, "verified");
    assert_eq!(old_physical.verification_receipt.as_deref(), Some(RECEIPT));
    assert_completed_backfill(&fixture.db).await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL (real PostgreSQL)"]
async fn postgres_exact_delete_wins_during_backfill_io_and_stale_receipt_is_rejected() {
    let fixture = PgResidencyFixture::new().await;
    create_bucket(
        &fixture.db,
        "delete-io-winner",
        BucketVersioningState::Enabled,
    )
    .await;
    let publication = publish(
        &fixture.db,
        "delete-old-object",
        "delete-io-winner",
        "key",
        OLD_CID,
        false,
    )
    .await;
    let public_version_id = publication
        .version_id
        .expect("enabled publication exposes a version ID");
    let old_version = version_for_object(&fixture.db, "delete-old-object").await;
    let snapshot = claim_snapshot(&fixture.db, "backfill-delete-stale").await;

    let actor_db = fixture.connection().await;
    let io_arrived = Arc::new(Barrier::new(2));
    let io_resume = Arc::new(Notify::new());
    let actor_barrier = io_arrived.clone();
    let actor_resume = io_resume.clone();
    let actor_snapshot = snapshot.clone();
    let actor = tokio::spawn(async move {
        actor_barrier.wait().await;
        actor_resume.notified().await;
        finish_after_stale_owner(&actor_db, &actor_snapshot).await;
    });

    io_arrived.wait().await;
    guarded_exact_delete(&fixture.db, "delete-io-winner", "key", &public_version_id).await;
    io_resume.notify_one();
    actor.await.expect("join stale exact-delete backfill actor");

    assert!(
        object_version::Entity::find_by_id(&old_version.id)
            .one(&fixture.db)
            .await
            .expect("query exactly deleted version")
            .is_none()
    );
    let old_physical = physical(&fixture.db, OLD_CID).await;
    assert_eq!(old_physical.verification_state, "pending");
    assert!(old_physical.verification_receipt.is_none());
    assert_completed_backfill(&fixture.db).await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL (real PostgreSQL)"]
async fn postgres_backfill_writeback_blocks_exact_delete_then_preserves_only_physical_receipt() {
    let fixture = PgResidencyFixture::new().await;
    create_bucket(
        &fixture.db,
        "delete-writeback-winner",
        BucketVersioningState::Enabled,
    )
    .await;
    let publication = publish(
        &fixture.db,
        "delete-locked-object",
        "delete-writeback-winner",
        "key",
        OLD_CID,
        false,
    )
    .await;
    let public_version_id = publication
        .version_id
        .expect("enabled publication exposes a version ID");
    let old_version = version_for_object(&fixture.db, "delete-locked-object").await;
    let snapshot = claim_snapshot(&fixture.db, "backfill-delete-first").await;

    let verifier_db = fixture.connection().await;
    let verifier_pid = backend_pid(&verifier_db).await;
    let transaction = verifier_db
        .begin()
        .await
        .expect("begin verification transaction");
    assert!(
        mark_hot_verified_in_transaction(&transaction, &snapshot.identity, NODE, RECEIPT)
            .await
            .expect("mark exact-delete owner verified")
    );

    let contender_db = fixture.connection().await;
    let contender_pid = backend_pid(&contender_db).await;
    let deletion_started = Arc::new(Notify::new());
    let actor_started = deletion_started.clone();
    let contender = tokio::spawn(async move {
        let now = database_now(&contender_db)
            .await
            .expect("read delete database clock");
        let guard = admit_content_mutation(
            &contender_db,
            "delete-writeback-winner",
            "key",
            None,
            SupersedeReason::DeleteObject,
            now,
        )
        .await
        .expect("admit contending exact delete");
        actor_started.notify_one();
        delete_version_with_leases_guarded(
            &contender_db,
            "delete-writeback-winner",
            "key",
            VersionSelector::Exact(
                PublicVersionId::parse_s3(&public_version_id)
                    .expect("valid exact-delete public version ID"),
            ),
            guard,
            now,
        )
        .await
        .expect("finish contending exact delete")
    });

    deletion_started.notified().await;
    wait_until_blocked_by(&fixture.db, contender_pid, verifier_pid).await;
    assert!(
        checkpoint_residency_backfill_in_transaction(
            &transaction,
            &snapshot.claim,
            Some(&snapshot.next_cursor),
            snapshot.completes_pass,
        )
        .await
        .expect("checkpoint writeback-first exact delete"),
        "receipt and checkpoint must share the current epoch"
    );
    transaction
        .commit()
        .await
        .expect("commit writeback before exact delete");
    contender.await.expect("join blocked exact-delete actor");

    assert!(
        object_version::Entity::find_by_id(&old_version.id)
            .one(&fixture.db)
            .await
            .expect("query exactly deleted owner")
            .is_none()
    );
    assert!(
        version_residency::Entity::find_by_id(&old_version.id)
            .one(&fixture.db)
            .await
            .expect("query released exact-delete residency")
            .is_none()
    );
    let old_physical = physical(&fixture.db, OLD_CID).await;
    assert_eq!(old_physical.verification_state, "verified");
    assert_eq!(old_physical.verification_receipt.as_deref(), Some(RECEIPT));
    assert_completed_backfill(&fixture.db).await;
    fixture.cleanup().await;
}

fn lifecycle_worker_config() -> ipfs_s3_gateway::config::ValidatedLifecycleConfig {
    LifecycleWorkerConfig {
        poll_interval_ms: 10,
        scan_page_size: 100,
        scan_lease_secs: 30,
        action_lease_secs: 30,
        worker_concurrency: 1,
        max_attempts: 2,
        base_backoff_secs: 1,
        max_backoff_secs: 1,
    }
    .validate()
    .expect("validate PostgreSQL lifecycle worker config")
}

async fn wait_for_terminal_action(
    db: &DatabaseConnection,
    action_id: &str,
) -> lifecycle_action::Model {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let action = lifecycle_action::Entity::find_by_id(action_id)
                .one(db)
                .await
                .expect("observe lifecycle action")
                .expect("lifecycle action exists");
            if matches!(
                action.state.as_str(),
                "succeeded" | "cancelled" | "failed_safe"
            ) {
                return action;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("lifecycle action must become terminal")
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL (real PostgreSQL)"]
async fn postgres_lifecycle_expiration_releases_only_selected_shared_cid_owner() {
    const SHARED_CID: &str = "bafy-pg-lifecycle-shared";
    let fixture = PgResidencyFixture::new().await;
    let bucket = "lifecycle-shared-owner";
    create_bucket(&fixture.db, bucket, BucketVersioningState::Enabled).await;
    publish(
        &fixture.db,
        "lifecycle-old-object",
        bucket,
        "key",
        SHARED_CID,
        true,
    )
    .await;
    publish(
        &fixture.db,
        "lifecycle-current-object",
        bucket,
        "key",
        SHARED_CID,
        true,
    )
    .await;
    let old_version = version_for_object(&fixture.db, "lifecycle-old-object").await;
    let current_version = version_for_object(&fixture.db, "lifecycle-current-object").await;

    fixture
        .db
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "UPDATE object_versions SET became_noncurrent_at = clock_timestamp() - INTERVAL '3 days', \
             lifecycle_age_started_at = clock_timestamp() - INTERVAL '4 days', \
             updated_at = clock_timestamp() WHERE id = $1",
            [old_version.id.clone().into()],
        ))
        .await
        .expect("age selected noncurrent lifecycle owner");
    let configuration = CanonicalLifecycleConfiguration {
        schema_version: 1,
        rules: vec![CanonicalLifecycleRule {
            id: Some("expire-selected-owner".to_owned()),
            status: LifecycleRuleStatus::Enabled,
            selector: ipfs_s3_gateway::lifecycle::model::CanonicalRuleSelector::Modern {
                filter: CanonicalFilter::All,
            },
            expiration: None,
            noncurrent_version_expiration: Some(NoncurrentExpiration {
                noncurrent_days: 1,
                newer_noncurrent_versions: None,
            }),
            abort_incomplete_multipart_upload: None,
            transition: None,
            noncurrent_version_transition: None,
        }],
    };
    put_configuration(
        &fixture.db,
        bucket,
        &canonical_json(&configuration).expect("serialize lifecycle fixture configuration"),
    )
    .await
    .expect("configure shared-CID lifecycle expiration");

    let worker_store = Store::new(fixture.connection().await);
    let gate = LifecycleAfterClaimGate::new("shared-cid-lifecycle-worker");
    let worker = start_worker_for_test(
        worker_store,
        lifecycle_worker_config(),
        CancellationToken::new(),
        LifecycleWorkerTestControl {
            worker_id: "shared-cid-lifecycle-worker".to_owned(),
            after_claim: Some(gate.clone()),
        },
    );
    let claim = tokio::time::timeout(Duration::from_secs(15), gate.wait_claim())
        .await
        .expect("lifecycle worker must claim selected shared-CID owner");
    assert_eq!(claim.action.action_kind, "expire_noncurrent");
    assert_eq!(
        claim.action.target_version_row_id.as_deref(),
        Some(old_version.id.as_str())
    );
    assert_eq!(
        claim.action.target_object_id.as_deref(),
        Some("lifecycle-old-object")
    );
    // Hold the surviving owner's verification writeback open while the other
    // connection expires the noncurrent owner of the same physical CID.
    let verifier_db = fixture.connection().await;
    let verify = verifier_db
        .begin()
        .await
        .expect("begin shared-CID verification");
    assert!(
        mark_hot_verified_in_transaction(
            &verify,
            &VersionResidencyIdentity::new(
                &current_version.id,
                "lifecycle-current-object",
                SHARED_CID,
            ),
            NODE,
            RECEIPT,
        )
        .await
        .expect("verify surviving owner's shared physical residency")
    );
    gate.release();
    let terminal = wait_for_terminal_action(&fixture.db, &claim.action.id).await;
    assert_eq!(terminal.state, "succeeded");
    verify
        .commit()
        .await
        .expect("commit receipt after selected owner expires");
    worker.shutdown(Duration::from_secs(2)).await;

    assert!(
        object_version::Entity::find_by_id(&old_version.id)
            .one(&fixture.db)
            .await
            .expect("query expired selected version")
            .is_none()
    );
    assert!(
        version_residency::Entity::find_by_id(&old_version.id)
            .one(&fixture.db)
            .await
            .expect("query expired selected residency")
            .is_none()
    );
    assert!(
        object_version::Entity::find_by_id(&current_version.id)
            .one(&fixture.db)
            .await
            .expect("query surviving shared-CID version")
            .is_some()
    );
    let remaining = resolve_version_residency(&fixture.db, &current_version.id)
        .await
        .expect("resolve surviving shared-CID version");
    assert_eq!(
        remaining.physical.verification_state,
        VerificationState::Verified
    );
    assert_eq!(
        remaining.physical.verification_receipt.as_deref(),
        Some(RECEIPT)
    );

    let retained = residency_reference::Entity::find()
        .filter(residency_reference::Column::Reason.eq("retained_version"))
        .filter(residency_reference::Column::Cid.eq(SHARED_CID))
        .all(&fixture.db)
        .await
        .expect("load surviving shared-CID references");
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].version_row_id, current_version.id);

    let old_lease = pin_lease::Entity::find()
        .filter(pin_lease::Column::OwnerObjectId.eq("lifecycle-old-object"))
        .one(&fixture.db)
        .await
        .expect("load selected owner's lease")
        .expect("selected owner's lease remains auditable");
    let current_lease = pin_lease::Entity::find()
        .filter(pin_lease::Column::OwnerObjectId.eq("lifecycle-current-object"))
        .one(&fixture.db)
        .await
        .expect("load surviving owner's lease")
        .expect("surviving owner's lease exists");
    assert_ne!(old_lease.state, "active");
    assert_eq!(current_lease.state, "active");
    let active_target = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(&current_lease.id))
        .filter(pin_lease_target::Column::Cid.eq(SHARED_CID))
        .one(&fixture.db)
        .await
        .expect("load surviving shared-CID lease target")
        .expect("surviving shared-CID lease target exists");
    assert!(matches!(
        active_target.state.as_str(),
        "waiting" | "submitted" | "pinned" | "degraded" | "quota_waiting" | "quota_blocked"
    ));
    assert_eq!(
        physical_residency::Entity::find()
            .filter(physical_residency::Column::Tier.eq("hot"))
            .filter(physical_residency::Column::Cid.eq(SHARED_CID))
            .count(&fixture.db)
            .await
            .expect("count shared physical residency"),
        1
    );
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL (real PostgreSQL)"]
async fn postgres_backfill_epoch_takeover_fences_old_receipt_checkpoint_and_release() {
    let fixture = PgResidencyFixture::new().await;
    create_bucket(
        &fixture.db,
        "epoch-takeover",
        BucketVersioningState::Enabled,
    )
    .await;
    publish(
        &fixture.db,
        "epoch-object",
        "epoch-takeover",
        "key",
        OLD_CID,
        false,
    )
    .await;
    let snapshot = claim_snapshot(&fixture.db, "old-backfill-actor").await;

    let old_db = fixture.connection().await;
    let io_arrived = Arc::new(Barrier::new(2));
    let io_resume = Arc::new(Notify::new());
    let actor_barrier = io_arrived.clone();
    let actor_resume = io_resume.clone();
    let actor_snapshot = snapshot.clone();
    let old_actor = tokio::spawn(async move {
        actor_barrier.wait().await;
        actor_resume.notified().await;

        let transaction = old_db.begin().await.expect("begin old-epoch writeback");
        let marked =
            mark_hot_verified_in_transaction(&transaction, &actor_snapshot.identity, NODE, RECEIPT)
                .await
                .expect("old epoch can revalidate its still-live immutable owner");
        let checkpointed = checkpoint_residency_backfill_in_transaction(
            &transaction,
            &actor_snapshot.claim,
            Some(&actor_snapshot.next_cursor),
            actor_snapshot.completes_pass,
        )
        .await
        .expect("fence old-epoch checkpoint");
        assert!(
            !checkpointed,
            "old epoch must not checkpoint replacement claim"
        );
        transaction
            .rollback()
            .await
            .expect("rollback old epoch receipt with rejected checkpoint");

        let release = old_db.begin().await.expect("begin old-epoch release");
        let released = release_residency_backfill_in_transaction(&release, &actor_snapshot.claim)
            .await
            .expect("fence old-epoch release");
        assert!(!released, "old epoch must not release replacement claim");
        release
            .rollback()
            .await
            .expect("rollback rejected old-epoch release");
        marked
    });

    io_arrived.wait().await;
    fixture
        .db
        .execute_unprepared(
            "UPDATE residency_backfill SET lease_until = clock_timestamp() - INTERVAL '1 second' \
             WHERE id = 'hot_verification'",
        )
        .await
        .expect("expire only the old backfill fixture lease");
    let replacement = claim_residency_backfill(
        &fixture.db,
        "replacement-backfill-actor",
        ChronoDuration::seconds(30),
    )
    .await
    .expect("claim replacement backfill epoch")
    .expect("expired old epoch must be replaceable");
    assert_eq!(replacement.claim_epoch, snapshot.claim.claim_epoch + 1);
    io_resume.notify_one();
    assert!(
        old_actor.await.expect("join old backfill actor"),
        "the receipt mutation must execute before its epoch checkpoint is rejected"
    );

    let physical = physical(&fixture.db, OLD_CID).await;
    assert_eq!(physical.verification_state, "pending");
    assert!(physical.node_identity.is_none());
    assert!(physical.verification_receipt.is_none());
    assert!(physical.verified_at.is_none());
    let state = residency_backfill::Entity::find_by_id("hot_verification")
        .one(&fixture.db)
        .await
        .expect("load replacement backfill state")
        .expect("replacement backfill state exists");
    assert_eq!(state.claim_epoch, replacement.claim_epoch);
    assert_eq!(
        state.claimed_by.as_deref(),
        Some("replacement-backfill-actor")
    );
    assert_eq!(
        state.cursor_version_row_id.as_deref(),
        replacement
            .cursor
            .as_ref()
            .map(|cursor| cursor.version_row_id.as_str())
    );
    assert!(state.lease_until.is_some());
    assert!(!state.completed);

    let release = fixture.db.begin().await.expect("begin replacement release");
    assert!(
        release_residency_backfill_in_transaction(&release, &replacement)
            .await
            .expect("release replacement backfill epoch")
    );
    release
        .commit()
        .await
        .expect("commit replacement backfill release");
    fixture.cleanup().await;
}
