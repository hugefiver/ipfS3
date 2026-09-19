#[path = "support/pg_residency.rs"]
#[allow(dead_code)]
mod pg_residency;

use std::time::Duration;

use chrono::Duration as ChronoDuration;
use ipfs_s3_gateway::{
    lifecycle::model::ClaimedLifecycleAction,
    store::{
        entities::{lifecycle_action, lifecycle_transition},
        import::ownership::StandardMutationGuard,
        lifecycle_action::{MAX_LIFECYCLE_ACTION_ATTEMPTS, claim_due, renew_claim},
        lifecycle_transition::{
            TierCopyReceipt, TransitionPublishResult, publish, record_verified,
        },
    },
};
use sea_orm::{ConnectionTrait, EntityTrait, TransactionTrait};

use pg_residency::{PgResidencyFixture, backend_pid, wait_until_blocked_by};

async fn seed_action(
    db: &sea_orm::DatabaseConnection,
    id: &str,
    state: &str,
    attempts: i64,
    epoch: i64,
    worker: Option<&str>,
    lease_sql: &str,
) {
    let claimed_by = worker
        .map(|worker| format!("'{worker}'"))
        .unwrap_or_else(|| "NULL".to_owned());
    db.execute_unprepared(&format!(
        "INSERT INTO lifecycle_actions \
         (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
          target_type, target_version_row_id, target_public_version_id, target_object_id, \
          target_sequence, due_at, state, attempts, next_attempt_at, claim_epoch, lease_until, \
          claimed_by, created_at, updated_at) VALUES \
         ('{id}', '{id}-key', 'bucket', '{id}-key', 1, 'id:transition', \
          'transition_current', 'version', '{id}-version', '{id}-public', '{id}-object', 1, \
          clock_timestamp() - interval '1 second', '{state}', {attempts}, \
          clock_timestamp() - interval '1 second', {epoch}, {lease_sql}, {claimed_by}, \
          clock_timestamp(), clock_timestamp())"
    ))
    .await
    .unwrap();
}

async fn claim_model(
    db: &sea_orm::DatabaseConnection,
    id: &str,
    epoch: i64,
    worker: &str,
) -> ClaimedLifecycleAction {
    ClaimedLifecycleAction {
        action: lifecycle_action::Entity::find_by_id(id)
            .one(db)
            .await
            .unwrap()
            .unwrap(),
        claim_epoch: epoch,
        worker_id: worker.to_owned(),
    }
}

async fn seed_saga(db: &sea_orm::DatabaseConnection, action_id: &str, published: bool) {
    let (checkpoint, verification, publication) = if published {
        ("publish", "'verification-receipt'", "'publication-receipt'")
    } else {
        ("copy", "NULL", "NULL")
    };
    db.execute_unprepared(&format!(
        "INSERT INTO lifecycle_transitions \
         (id, action_id, action_kind, bucket, object_key, config_revision, rule_id, \
          target_version_row_id, target_public_version_id, target_object_id, target_sequence, \
          source_tier, destination_tier, source_cid, destination_cid, \
          source_residency_revision, expected_source_node_identity, \
          expected_destination_node_identity, ownership_generation, checkpoint, \
          verification_receipt, publication_receipt, created_at, updated_at) VALUES \
         ('{action_id}-saga', '{action_id}', 'transition_current', 'bucket', '{action_id}-key', \
          1, 'id:transition', '{action_id}-version', '{action_id}-public', \
          '{action_id}-object', 1, 'hot', 'cold', 'transition-cid', 'transition-cid', 1, \
          'hot-node', 'cold-node', 1, '{checkpoint}', {verification}, {publication}, \
          clock_timestamp(), clock_timestamp())"
    ))
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "NOT RUN by default: requires explicit IPFS_S3_TEST_POSTGRES_URL endpoint"]
async fn postgres_transition_claim_fences_cover_lock_wait_stale_epoch_and_cleanup_reclaim() {
    let fixture = PgResidencyFixture::new().await;
    fixture
        .db
        .execute_unprepared(
            "INSERT INTO buckets (name) VALUES ('bucket'); \
             INSERT INTO physical_residencies \
             (tier, cid, node_identity, verification_state, verification_receipt, verified_at, \
              created_at, updated_at) VALUES \
             ('hot', 'transition-cid', 'hot-node', 'verified', 'hot-receipt', clock_timestamp(), \
              clock_timestamp(), clock_timestamp())",
        )
        .await
        .unwrap();

    seed_action(
        &fixture.db,
        "renew-action",
        "claimed",
        1,
        1,
        Some("renew-worker"),
        "clock_timestamp() + interval '2 seconds'",
    )
    .await;
    let renew_claim_token = claim_model(&fixture.db, "renew-action", 1, "renew-worker").await;
    let contender = fixture.connection().await;
    let observer = fixture.connection().await;
    let blocker_pid = backend_pid(&fixture.db).await;
    let contender_pid = backend_pid(&contender).await;
    let blocker = fixture.db.begin().await.unwrap();
    blocker
        .execute_unprepared("SELECT id FROM lifecycle_actions WHERE id = 'renew-action' FOR UPDATE")
        .await
        .unwrap();
    let renew_task = tokio::spawn(async move {
        renew_claim(&contender, &renew_claim_token, ChronoDuration::seconds(30)).await
    });
    wait_until_blocked_by(&observer, contender_pid, blocker_pid).await;
    tokio::time::sleep(Duration::from_millis(2_200)).await;
    blocker.commit().await.unwrap();
    assert!(
        !renew_task.await.unwrap().unwrap(),
        "a lease that expires while renewal waits on the row lock must not be revived"
    );
    fixture
        .db
        .execute_unprepared(
            "UPDATE lifecycle_actions SET state = 'failed_safe', lease_until = NULL, \
             claimed_by = NULL, finished_at = clock_timestamp() WHERE id = 'renew-action'",
        )
        .await
        .unwrap();

    seed_action(
        &fixture.db,
        "stale-action",
        "claimed",
        2,
        2,
        Some("new-worker"),
        "clock_timestamp() + interval '30 seconds'",
    )
    .await;
    seed_saga(&fixture.db, "stale-action", false).await;
    let stale = claim_model(&fixture.db, "stale-action", 1, "old-worker").await;
    let saga = lifecycle_transition::Entity::find_by_id("stale-action-saga")
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let receipt = TierCopyReceipt {
        node_identity: "cold-node".to_owned(),
        cid: "transition-cid".to_owned(),
    };
    let guard = StandardMutationGuard {
        bucket: "bucket".to_owned(),
        key: "stale-action-key".to_owned(),
        mutation_id: "lifecycle:stale-action:1".to_owned(),
        expected_generation: 1,
        mutation_prefix: None,
    };
    let stale_txn = fixture.db.begin().await.unwrap();
    assert!(
        record_verified(&stale_txn, &stale, &saga, &receipt)
            .await
            .unwrap()
            .is_none(),
        "a stale epoch must not install verification evidence"
    );
    assert!(matches!(
        publish(&stale_txn, &stale, &guard, &receipt).await.unwrap(),
        TransitionPublishResult::Stale
    ));
    stale_txn.rollback().await.unwrap();

    seed_action(
        &fixture.db,
        "published-action",
        "claimed",
        MAX_LIFECYCLE_ACTION_ATTEMPTS,
        1,
        Some("crashed-worker"),
        "clock_timestamp() - interval '1 second'",
    )
    .await;
    seed_saga(&fixture.db, "published-action", true).await;
    let cleanup = claim_due(
        &fixture.db,
        "cleanup-worker",
        ChronoDuration::seconds(30),
        1,
    )
    .await
    .unwrap()
    .pop()
    .expect("published cleanup must reclaim beyond the attempt cap");
    assert_eq!(cleanup.action.id, "published-action");
    assert_eq!(cleanup.action.attempts, MAX_LIFECYCLE_ACTION_ATTEMPTS + 1);
    fixture
        .db
        .execute_unprepared(
            "UPDATE lifecycle_actions SET lease_until = clock_timestamp() - interval '1 second' \
             WHERE id = 'published-action'",
        )
        .await
        .unwrap();
    let cleanup_again = claim_due(
        &fixture.db,
        "cleanup-worker-2",
        ChronoDuration::seconds(30),
        1,
    )
    .await
    .unwrap()
    .pop()
    .expect("post-publication cleanup must remain reclaimable after another crash");
    assert_eq!(cleanup_again.action.id, "published-action");
    assert_eq!(cleanup_again.claim_epoch, cleanup.claim_epoch + 1);

    observer.close().await.unwrap();
    fixture.cleanup().await;
}
