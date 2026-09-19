use sea_orm::{
    ColumnTrait, ConnectionTrait, Database, DatabaseConnection, EntityTrait, PaginatorTrait,
    QueryFilter, TransactionTrait,
};

use super::*;
use crate::{
    import::SupersedeReason,
    residency::PhysicalVerification,
    store::{
        self,
        entities::{import_destination, object_version},
        import::ownership::{admit_content_mutation, try_admit_lifecycle_mutation},
    },
};

const CID: &str = "bafy-transition-store-test";
const HOT_NODE: &str = "hot-node";
const COLD_NODE: &str = "cold-node";

#[tokio::test]
async fn settled_receipt_does_not_permanently_prevent_empty_bucket_deletion() {
    let db = db().await;
    let (claim, guard, saga) = published_saga_with_guard(&db, "bucket-cleanup").await;
    let txn = db.begin().await.unwrap();
    assert!(cleanup(&txn, &claim, &guard, &saga).await.unwrap());
    txn.commit().await.unwrap();
    db.execute_unprepared("DELETE FROM object_versions")
        .await
        .unwrap();
    db.execute_unprepared("UPDATE objects SET is_latest = FALSE")
        .await
        .unwrap();
    store::bucket::delete(&db, "bucket").await.unwrap();
    assert!(!store::bucket::exists(&db, "bucket").await.unwrap());
    assert_eq!(
        physical_residency::Entity::find().count(&db).await.unwrap(),
        2
    );
}

async fn db() -> DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .unwrap();
    store::run_migrations(&db).await.unwrap();
    db
}

async fn seed_claim(
    db: &DatabaseConnection,
    action_id: &str,
    epoch: i64,
) -> ClaimedLifecycleAction {
    db.execute_unprepared("INSERT INTO buckets (name) VALUES ('bucket')")
        .await
        .unwrap();
    db.execute_unprepared(&format!(
        "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest, created_at) \
         VALUES ('object', 'bucket', 'key', '{CID}', 7, '{CID}', TRUE, CURRENT_TIMESTAMP)"
    ))
    .await
    .unwrap();
    db.execute_unprepared(
        "INSERT INTO object_versions \
         (id, bucket, key, version_id, kind, object_id, sequence, is_latest, \
          lifecycle_age_started_at, created_at, updated_at) VALUES \
         ('version', 'bucket', 'key', 'public-version', 'object', 'object', 1, TRUE, \
          CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .await
    .unwrap();
    let txn = db.begin().await.unwrap();
    store::residency::attach_hot_in_transaction(
        &txn,
        &VersionResidencyIdentity::new("version", "object", CID),
        &PhysicalVerification::verified(HOT_NODE, "hot-receipt"),
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    db.execute_unprepared(&format!(
        "INSERT INTO lifecycle_actions \
         (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
          target_type, target_version_row_id, target_public_version_id, target_object_id, \
          target_sequence, due_at, state, attempts, next_attempt_at, claim_epoch, lease_until, \
          claimed_by, created_at, updated_at) VALUES \
         ('{action_id}', '{action_id}-key', 'bucket', 'key', 1, 'id:transition', \
          'transition_current', 'version', 'version', 'public-version', 'object', 1, \
          CURRENT_TIMESTAMP, 'claimed', 1, CURRENT_TIMESTAMP, {epoch}, \
          '2999-01-01T00:00:00Z', 'worker', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
    ))
    .await
    .unwrap();
    let action = lifecycle_action::Entity::find_by_id(action_id)
        .one(db)
        .await
        .unwrap()
        .unwrap();
    ClaimedLifecycleAction {
        action,
        claim_epoch: epoch,
        worker_id: "worker".to_owned(),
    }
}

async fn prepared_saga(
    db: &DatabaseConnection,
    claim: &ClaimedLifecycleAction,
    generation: i64,
) -> lifecycle_transition::Model {
    let txn = db.begin().await.unwrap();
    let saga = insert_prepared_in_transaction(
        &txn,
        claim,
        PreparedLifecycleTransition {
            source_residency_revision: 1,
            expected_source_node_identity: HOT_NODE.to_owned(),
            expected_destination_node_identity: COLD_NODE.to_owned(),
            ownership_generation: generation,
        },
    )
    .await
    .unwrap()
    .unwrap();
    attach_transition_hold_in_transaction(
        &txn,
        &saga.id,
        ReferenceReason::TransitionStaging,
        &saga_identity(&saga),
        &ResidencyLocation::new(KuboTier::Hot, CID),
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    saga
}

fn receipt() -> TierCopyReceipt {
    TierCopyReceipt {
        node_identity: COLD_NODE.to_owned(),
        cid: CID.to_owned(),
    }
}

async fn admit(db: &DatabaseConnection, claim: &ClaimedLifecycleAction) -> StandardMutationGuard {
    try_admit_lifecycle_mutation(
        db,
        &claim.action.bucket,
        &claim.action.object_key,
        &claim.action.id,
        claim.claim_epoch,
        database_now(db).await.unwrap(),
    )
    .await
    .unwrap()
    .unwrap()
}

async fn supersede_with_put(
    db: &DatabaseConnection,
    claim: &ClaimedLifecycleAction,
) -> StandardMutationGuard {
    admit_content_mutation(
        db,
        &claim.action.bucket,
        &claim.action.object_key,
        None,
        SupersedeReason::PutObject,
        database_now(db).await.unwrap(),
    )
    .await
    .unwrap()
}

async fn published_saga_with_guard(
    db: &DatabaseConnection,
    action_id: &str,
) -> (
    ClaimedLifecycleAction,
    StandardMutationGuard,
    lifecycle_transition::Model,
) {
    let claim = seed_claim(db, action_id, 1).await;
    let guard = admit(db, &claim).await;
    let saga = prepared_saga(db, &claim, guard.expected_generation).await;
    let txn = db.begin().await.unwrap();
    let copied = record_copy(&txn, &claim, &saga).await.unwrap().unwrap();
    let verified = record_verified(&txn, &claim, &copied, &receipt())
        .await
        .unwrap()
        .unwrap();
    validate_current_verification(&verified, &claim, &receipt()).unwrap();
    let published = publish_residency(&txn, &claim, &verified, &receipt())
        .await
        .unwrap();
    txn.commit().await.unwrap();
    (claim, guard, published)
}

async fn reclaim(
    db: &DatabaseConnection,
    claim: &ClaimedLifecycleAction,
) -> ClaimedLifecycleAction {
    db.execute_unprepared(&format!(
        "UPDATE lifecycle_actions SET claim_epoch = {}, lease_until = '2999-01-01T00:00:00Z' \
         WHERE id = '{}'",
        claim.claim_epoch + 1,
        claim.action.id
    ))
    .await
    .unwrap();
    let mut reclaimed = claim.clone();
    reclaimed.claim_epoch += 1;
    reclaimed.action.claim_epoch = reclaimed.claim_epoch;
    reclaimed
}

fn recovery_guard(
    claim: &ClaimedLifecycleAction,
    original: &StandardMutationGuard,
) -> StandardMutationGuard {
    StandardMutationGuard {
        bucket: original.bucket.clone(),
        key: original.key.clone(),
        mutation_id: format!("lifecycle:{}:{}", claim.action.id, claim.claim_epoch),
        expected_generation: original.expected_generation,
        mutation_prefix: None,
    }
}

#[tokio::test]
async fn verification_is_bound_to_current_claim_epoch_and_saga() {
    let db = db().await;
    let claim = seed_claim(&db, "transition", 1).await;
    let guard = admit(&db, &claim).await;
    let saga = prepared_saga(&db, &claim, guard.expected_generation).await;

    let txn = db.begin().await.unwrap();
    let copied = record_copy(&txn, &claim, &saga).await.unwrap().unwrap();
    let verified = record_verified(&txn, &claim, &copied, &receipt())
        .await
        .unwrap()
        .unwrap();
    txn.commit().await.unwrap();

    let durable: DurableVerificationReceipt =
        serde_json::from_str(verified.verification_receipt.as_deref().unwrap()).unwrap();
    assert_eq!(durable.saga_id, saga.id);
    assert_eq!(durable.action_id, claim.action.id);
    assert_eq!(durable.claim_epoch, 1);
    assert_eq!(durable.source_residency_revision, 1);
    assert_eq!(durable.tier_receipt, receipt());

    db.execute_unprepared("UPDATE lifecycle_actions SET claim_epoch = 2 WHERE id = 'transition'")
        .await
        .unwrap();
    let mut reclaimed = claim.clone();
    reclaimed.claim_epoch = 2;
    reclaimed.action.claim_epoch = 2;
    let _reclaimed_guard = admit(&db, &reclaimed).await;
    let txn = db.begin().await.unwrap();
    let verified = record_verified(&txn, &reclaimed, &verified, &receipt())
        .await
        .unwrap()
        .unwrap();
    txn.commit().await.unwrap();
    let durable: DurableVerificationReceipt =
        serde_json::from_str(verified.verification_receipt.as_deref().unwrap()).unwrap();
    assert_eq!(durable.claim_epoch, 2);
}

#[tokio::test]
async fn superseded_owner_cannot_record_copy_or_clear_the_new_put_guard() {
    let db = db().await;
    let claim = seed_claim(&db, "stale-copy", 1).await;
    let initial = admit(&db, &claim).await;
    let saga = prepared_saga(&db, &claim, initial.expected_generation).await;
    let put_guard = supersede_with_put(&db, &claim).await;

    let txn = db.begin().await.unwrap();
    assert!(record_copy(&txn, &claim, &saga).await.unwrap().is_none());
    txn.commit().await.unwrap();

    let stored = lifecycle_transition::Entity::find_by_id(&saga.id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.checkpoint, "prepare");
    assert!(stored.verification_receipt.is_none());
    let destination =
        import_destination::Entity::find_by_id(("bucket".to_owned(), "key".to_owned()))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(destination.mutation_id, Some(put_guard.mutation_id));
    assert_eq!(destination.generation, put_guard.expected_generation);
    assert!(
        physical_residency::Entity::find_by_id(("cold".to_owned(), CID.to_owned()))
            .one(&db)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn superseded_owner_cannot_record_verification_or_clear_the_new_put_guard() {
    let db = db().await;
    let claim = seed_claim(&db, "stale-verify", 1).await;
    let initial = admit(&db, &claim).await;
    let saga = prepared_saga(&db, &claim, initial.expected_generation).await;
    let txn = db.begin().await.unwrap();
    let copied = record_copy(&txn, &claim, &saga).await.unwrap().unwrap();
    txn.commit().await.unwrap();
    let put_guard = supersede_with_put(&db, &claim).await;

    let txn = db.begin().await.unwrap();
    assert!(
        record_verified(&txn, &claim, &copied, &receipt())
            .await
            .unwrap()
            .is_none()
    );
    txn.commit().await.unwrap();

    let stored = lifecycle_transition::Entity::find_by_id(&saga.id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.checkpoint, "copy");
    assert!(stored.verification_receipt.is_none());
    let cold = physical_residency::Entity::find_by_id(("cold".to_owned(), CID.to_owned()))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cold.verification_state, "pending");
    let destination =
        import_destination::Entity::find_by_id(("bucket".to_owned(), "key".to_owned()))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(destination.mutation_id, Some(put_guard.mutation_id));
    assert_eq!(destination.generation, put_guard.expected_generation);
}

#[tokio::test]
async fn cold_publication_moves_only_the_version_reference_and_keeps_hot_physical_state() {
    let db = db().await;
    let claim = seed_claim(&db, "publish-transition", 1).await;
    let guard = admit(&db, &claim).await;
    let saga = prepared_saga(&db, &claim, guard.expected_generation).await;
    let txn = db.begin().await.unwrap();
    let copied = record_copy(&txn, &claim, &saga).await.unwrap().unwrap();
    let verified = record_verified(&txn, &claim, &copied, &receipt())
        .await
        .unwrap()
        .unwrap();
    validate_current_verification(&verified, &claim, &receipt()).unwrap();
    let published = publish_residency(&txn, &claim, &verified, &receipt())
        .await
        .unwrap();
    txn.commit().await.unwrap();

    assert_eq!(published.checkpoint, "publish");
    let residency = version_residency::Entity::find_by_id("version")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(residency.primary_tier, "cold");
    assert_eq!(residency.storage_class, "STANDARD_IA");
    assert_eq!(residency.revision, 2);
    assert!(
        physical_residency::Entity::find_by_id(("hot".to_owned(), CID.to_owned()))
            .one(&db)
            .await
            .unwrap()
            .is_some(),
        "logical cleanup must never remove or unpin shared hot physical state"
    );
    assert_eq!(
        residency_reference::Entity::find()
            .filter(residency_reference::Column::OwnerKind.eq(OWNER_VERSION))
            .filter(residency_reference::Column::OwnerId.eq("version"))
            .filter(residency_reference::Column::Tier.eq("cold"))
            .count(&db)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        residency_reference::Entity::find()
            .filter(residency_reference::Column::OwnerKind.eq(OWNER_VERSION))
            .filter(residency_reference::Column::OwnerId.eq("version"))
            .filter(residency_reference::Column::Tier.eq("hot"))
            .count(&db)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn cancellation_releases_only_saga_holds_and_clears_its_current_guard() {
    let db = db().await;
    let claim = seed_claim(&db, "cancel-transition", 1).await;
    let now = database_now(&db).await.unwrap();
    let guard = try_admit_lifecycle_mutation(
        &db,
        "bucket",
        "key",
        &claim.action.id,
        claim.claim_epoch,
        now,
    )
    .await
    .unwrap()
    .unwrap();
    let saga = prepared_saga(&db, &claim, guard.expected_generation).await;
    let txn = db.begin().await.unwrap();
    assert!(
        settle_cancelled(&txn, &claim, &guard, Some(&saga))
            .await
            .unwrap()
    );
    txn.commit().await.unwrap();

    let action = lifecycle_action::Entity::find_by_id(&claim.action.id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(action.state, "cancelled");
    assert_eq!(
        residency_reference::Entity::find()
            .filter(residency_reference::Column::OwnerKind.eq(OWNER_TRANSITION))
            .filter(residency_reference::Column::OwnerId.eq(&saga.id))
            .count(&db)
            .await
            .unwrap(),
        0
    );
    assert!(
        object_version::Entity::find_by_id("version")
            .one(&db)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn recovery_cleanup_clears_an_older_same_action_guard_after_target_deletion() {
    let db = db().await;
    let (claim, original_guard, published) =
        published_saga_with_guard(&db, "recovery-cleanup").await;
    db.execute_unprepared("DELETE FROM object_versions WHERE id = 'version'")
        .await
        .unwrap();
    db.execute_unprepared("DELETE FROM objects WHERE id = 'object'")
        .await
        .unwrap();
    let reclaimed = reclaim(&db, &claim).await;
    let guard = recovery_guard(&reclaimed, &original_guard);

    let txn = db.begin().await.unwrap();
    assert!(cleanup(&txn, &reclaimed, &guard, &published).await.unwrap());
    txn.commit().await.unwrap();

    let destination = import_destination::Entity::find_by_id((
        original_guard.bucket.clone(),
        original_guard.key.clone(),
    ))
    .one(&db)
    .await
    .unwrap()
    .unwrap();
    assert_eq!(destination.mutation_id, None);
    assert_eq!(destination.mutation_prefix, None);
    let action = lifecycle_action::Entity::find_by_id(&claim.action.id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(action.state, "succeeded");
}

#[tokio::test]
async fn recovery_cleanup_never_clears_a_newer_unrelated_guard() {
    let db = db().await;
    let (claim, original_guard, published) =
        published_saga_with_guard(&db, "unrelated-cleanup").await;
    let reclaimed = reclaim(&db, &claim).await;
    db.execute_unprepared(
        "UPDATE import_destinations SET generation = generation + 1, \
         mutation_id = 'standard:unrelated', updated_at = CURRENT_TIMESTAMP \
         WHERE bucket = 'bucket' AND key = 'key'",
    )
    .await
    .unwrap();
    let guard = recovery_guard(&reclaimed, &original_guard);

    let txn = db.begin().await.unwrap();
    assert!(cleanup(&txn, &reclaimed, &guard, &published).await.unwrap());
    txn.commit().await.unwrap();

    let destination = import_destination::Entity::find_by_id((
        original_guard.bucket.clone(),
        original_guard.key.clone(),
    ))
    .one(&db)
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        destination.mutation_id.as_deref(),
        Some("standard:unrelated")
    );
    assert_eq!(
        destination.generation,
        original_guard.expected_generation + 1
    );
}

#[tokio::test]
async fn cleanup_rejects_a_non_binding_publication_receipt_without_settling() {
    let db = db().await;
    let (claim, guard, published) = published_saga_with_guard(&db, "bad-receipt-cleanup").await;
    db.execute_unprepared(
        "UPDATE lifecycle_transitions SET publication_receipt = '{\"version\":1}' \
         WHERE action_id = 'bad-receipt-cleanup'",
    )
    .await
    .unwrap();
    let malformed = lifecycle_transition::Entity::find_by_id(&published.id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();

    let txn = db.begin().await.unwrap();
    assert!(cleanup(&txn, &claim, &guard, &malformed).await.is_err());
    txn.rollback().await.unwrap();

    let action = lifecycle_action::Entity::find_by_id(&claim.action.id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(action.state, "claimed");
    let saga = lifecycle_transition::Entity::find_by_id(&published.id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saga.checkpoint, "publish");
    assert_eq!(saga.settlement_kind, None);
}
