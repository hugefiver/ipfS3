use ipfs_s3_gateway::{
    lifecycle::model::ClaimedLifecycleAction,
    residency::{PhysicalVerification, VersionResidencyIdentity},
    store::{
        self,
        database_clock::database_now,
        entities::{lifecycle_action, lifecycle_transition},
        lifecycle_transition::{PreparedLifecycleTransition, insert_prepared_in_transaction},
        migrations::*,
    },
};
use sea_orm::{
    ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, EntityTrait, Statement,
    TransactionTrait,
};
use sea_orm_migration::{MigrationTrait, MigratorTrait, SchemaManager};

struct PreTransitionMigrator;

impl MigratorTrait for PreTransitionMigrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20250701_000001_init::Migration),
            Box::new(m20260707_000001_decompress_zip::Migration),
            Box::new(m20260720_000001_sse_c_key_fingerprint::Migration),
            Box::new(m20260721_000001_multi_provider_pinning::Migration),
            Box::new(m20260729_000001_ipfs3_import::Migration),
            Box::new(m20260729_000002_postgres_utc_timestamps::Migration),
            Box::new(m20260730_000001_standard_mutation_fence::Migration),
            Box::new(m20260813_000001_postgres_json_columns::Migration),
            Box::new(m20260825_000001_object_versioning::Migration),
            Box::new(m20260826_000001_lifecycle_expiration::Migration),
            Box::new(m20260831_000001_bucket_cors::Migration),
            Box::new(m20260901_000001_lifecycle_abort_multipart::Migration),
            Box::new(m20260912_000001_residency_references::Migration),
        ]
    }
}

async fn sqlite() -> DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .unwrap();
    db
}

async fn seed_bucket_and_content(db: &DatabaseConnection) {
    db.execute_unprepared("INSERT INTO buckets (name) VALUES ('bucket')")
        .await
        .unwrap();
    db.execute_unprepared(
        "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest, created_at) \
         VALUES ('object', 'bucket', 'key', 'QmTransition', 7, 'QmTransition', TRUE, \
                 '2026-09-01T00:00:00Z')",
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "INSERT INTO object_versions \
         (id, bucket, key, version_id, kind, object_id, sequence, is_latest, \
          lifecycle_age_started_at, created_at, updated_at) VALUES \
         ('version', 'bucket', 'key', 'version-public', 'object', 'object', 1, TRUE, \
          '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z')",
    )
    .await
    .unwrap();
}

async fn seed_phase_ab_actions(db: &DatabaseConnection) {
    for statement in [
        "INSERT INTO lifecycle_actions \
         (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
          target_type, target_version_row_id, target_public_version_id, target_object_id, \
          target_sequence, target_upload_id, target_upload_created_at, due_at, state, attempts, \
          next_attempt_at, claim_epoch, lease_until, claimed_by, failure_class, \
          last_error_redacted, created_at, updated_at, finished_at) VALUES \
         ('phase-a', 'phase-a-key', 'bucket', 'key', 3, 'id:expire', 'expire_current', \
          'version', 'version', 'version-public', 'object', 1, NULL, NULL, \
          '2026-09-02T00:00:00Z', 'claimed', 4, '2026-09-02T00:01:00Z', 8, \
          '2026-09-02T00:05:00Z', 'worker-a', 'database_contention', \
          'lifecycle action failed', '2026-09-01T00:00:00Z', '2026-09-02T00:00:30Z', NULL)",
        "INSERT INTO lifecycle_actions \
         (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
          target_type, target_version_row_id, target_public_version_id, target_object_id, \
          target_sequence, target_upload_id, target_upload_created_at, due_at, state, attempts, \
          next_attempt_at, claim_epoch, lease_until, claimed_by, failure_class, \
          last_error_redacted, created_at, updated_at, finished_at) VALUES \
         ('phase-b', 'phase-b-key', 'bucket', 'upload', 3, 'id:abort', \
          'abort_incomplete_multipart_upload', 'multipart_upload', NULL, NULL, NULL, NULL, \
          'upload-id', '2026-08-01T00:00:00Z', '2026-09-02T00:00:00Z', 'failed_safe', 8, \
          '2026-09-02T00:00:00Z', 9, NULL, NULL, 'internal_dependency', \
          'lifecycle action failed', '2026-08-01T00:00:00Z', '2026-09-02T00:00:30Z', \
          '2026-09-02T00:00:30Z')",
    ] {
        db.execute_unprepared(statement).await.unwrap();
    }
}

async fn action_snapshot(db: &DatabaseConnection) -> Vec<u8> {
    db.query_all(Statement::from_string(
        DatabaseBackend::Sqlite,
        "SELECT json_array(\
             id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
             target_type, target_version_row_id, target_public_version_id, target_object_id, \
             target_sequence, target_upload_id, target_upload_created_at, due_at, state, attempts, \
             next_attempt_at, claim_epoch, lease_until, claimed_by, failure_class, \
             last_error_redacted, created_at, updated_at, finished_at) AS snapshot \
         FROM lifecycle_actions ORDER BY id",
    ))
    .await
    .unwrap()
    .into_iter()
    .flat_map(|row| {
        let mut bytes = row.try_get::<String>("", "snapshot").unwrap().into_bytes();
        bytes.push(b'\n');
        bytes
    })
    .collect()
}

async fn action_index_snapshot(db: &DatabaseConnection) -> Vec<String> {
    db.query_all(Statement::from_string(
        DatabaseBackend::Sqlite,
        "SELECT sql FROM sqlite_master WHERE type = 'index' AND tbl_name = 'lifecycle_actions' \
         AND sql IS NOT NULL ORDER BY name",
    ))
    .await
    .unwrap()
    .into_iter()
    .map(|row| row.try_get::<String>("", "sql").unwrap())
    .collect()
}

async fn assert_rejected(db: &DatabaseConnection, sql: &str) {
    assert!(
        db.execute_unprepared(sql).await.is_err(),
        "SQLite accepted illegal lifecycle transition shape: {sql}"
    );
}

#[tokio::test]
async fn sqlite_upgrade_preserves_phase_ab_rows_and_indexes_byte_for_byte() {
    let db = sqlite().await;
    PreTransitionMigrator::up(&db, None).await.unwrap();
    seed_bucket_and_content(&db).await;
    seed_phase_ab_actions(&db).await;
    let rows_before = action_snapshot(&db).await;
    let indexes_before = action_index_snapshot(&db).await;

    m20260912_000002_lifecycle_transition::Migration
        .up(&SchemaManager::new(&db))
        .await
        .unwrap();

    assert_eq!(action_snapshot(&db).await, rows_before);
    let indexes_after = action_index_snapshot(&db).await;
    for index in indexes_before {
        assert!(
            indexes_after.contains(&index),
            "lost Phase A/B index: {index}"
        );
    }
    assert_rejected(
        &db,
        "UPDATE lifecycle_actions SET target_sequence = -1 WHERE id = 'phase-a'",
    )
    .await;
    assert_rejected(
        &db,
        "UPDATE lifecycle_actions SET state = 'claimed', claimed_by = NULL WHERE id = 'phase-a'",
    )
    .await;
    assert_rejected(
        &db,
        "UPDATE lifecycle_actions SET target_object_id = 'hybrid' WHERE id = 'phase-b'",
    )
    .await;
    assert_rejected(
        &db,
        "UPDATE lifecycle_actions SET finished_at = NULL WHERE id = 'phase-b'",
    )
    .await;
}

#[tokio::test]
async fn fresh_schema_accepts_only_content_version_transition_actions_and_legal_sagas() {
    let db = sqlite().await;
    store::run_migrations(&db).await.unwrap();
    seed_bucket_and_content(&db).await;

    db.execute_unprepared(
        "INSERT INTO lifecycle_actions \
         (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
          target_type, target_version_row_id, target_public_version_id, target_object_id, \
          target_sequence, due_at, state, next_attempt_at, created_at, updated_at) VALUES \
         ('transition-current', 'transition-current-key', 'bucket', 'key', 1, 'id:t', \
          'transition_current', 'version', 'version', 'version-public', 'object', 1, \
          CURRENT_TIMESTAMP, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .await
    .unwrap();

    assert_rejected(
        &db,
        "INSERT INTO lifecycle_actions \
         (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
          target_type, target_version_row_id, target_public_version_id, target_object_id, \
          target_sequence, due_at, state, next_attempt_at, created_at, updated_at) VALUES \
         ('marker-transition', 'marker-transition-key', 'bucket', 'marker', 1, 'id:t', \
          'transition_current', 'version', 'marker-row', 'marker-public', NULL, 2, \
          CURRENT_TIMESTAMP, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .await;
    assert_rejected(
        &db,
        "UPDATE lifecycle_actions SET target_type = 'multipart_upload', \
         target_version_row_id = NULL, target_public_version_id = NULL, target_object_id = NULL, \
         target_sequence = NULL, target_upload_id = 'upload', \
         target_upload_created_at = CURRENT_TIMESTAMP WHERE id = 'transition-current'",
    )
    .await;

    assert_rejected(
        &db,
        "INSERT INTO lifecycle_transitions \
         (id, action_id, action_kind, bucket, object_key, config_revision, rule_id, \
          target_version_row_id, target_public_version_id, target_object_id, target_sequence, \
          source_tier, destination_tier, source_cid, destination_cid, \
          source_residency_revision, expected_source_node_identity, \
          expected_destination_node_identity, ownership_generation, checkpoint, \
          created_at, updated_at) VALUES \
         ('bad-saga', 'transition-current', 'transition_current', 'bucket', 'key', 1, 'id:t', \
          'version', 'version-public', 'object', 1, 'hot', 'cold', 'QmTransition', 'QmOther', \
          1, 'hot-node', 'cold-node', 1, 'prepare', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .await;
}

#[tokio::test]
async fn prepared_transition_insert_snapshots_action_residency_and_database_clock() {
    let db = sqlite().await;
    store::run_migrations(&db).await.unwrap();
    seed_bucket_and_content(&db).await;
    let identity = VersionResidencyIdentity::new("version", "object", "QmTransition");
    let txn = db.begin().await.unwrap();
    store::residency::attach_hot_in_transaction(
        &txn,
        &identity,
        &PhysicalVerification::Verified {
            node_identity: "hot-node".to_owned(),
            receipt: "hot-receipt".to_owned(),
        },
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    db.execute_unprepared(
        "INSERT INTO lifecycle_actions \
         (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
          target_type, target_version_row_id, target_public_version_id, target_object_id, \
          target_sequence, due_at, state, next_attempt_at, claim_epoch, lease_until, claimed_by, \
          created_at, updated_at) VALUES \
         ('transition', 'transition-key', 'bucket', 'key', 4, 'id:transition', \
          'transition_noncurrent', 'version', 'version', 'version-public', 'object', 1, \
          CURRENT_TIMESTAMP, 'claimed', CURRENT_TIMESTAMP, 1, '2999-01-01T00:00:00Z', 'worker', \
          CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .await
    .unwrap();
    let before = database_now(&db).await.unwrap();

    let prepared = PreparedLifecycleTransition {
        source_residency_revision: 1,
        expected_source_node_identity: "hot-node".to_owned(),
        expected_destination_node_identity: "cold-node".to_owned(),
        ownership_generation: 7,
    };
    let action = lifecycle_action::Entity::find_by_id("transition")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let claim = ClaimedLifecycleAction {
        action,
        claim_epoch: 1,
        worker_id: "worker".to_owned(),
    };
    let stale_claim = ClaimedLifecycleAction {
        action: claim.action.clone(),
        claim_epoch: claim.claim_epoch,
        worker_id: "stale-worker".to_owned(),
    };
    let stale_epoch = ClaimedLifecycleAction {
        action: claim.action.clone(),
        claim_epoch: claim.claim_epoch + 1,
        worker_id: claim.worker_id.clone(),
    };
    for stale in [&stale_claim, &stale_epoch] {
        let txn = db.begin().await.unwrap();
        assert!(
            insert_prepared_in_transaction(&txn, stale, prepared.clone())
                .await
                .unwrap()
                .is_none()
        );
        txn.commit().await.unwrap();
    }
    db.execute_unprepared(
        "UPDATE lifecycle_actions SET lease_until = '2000-01-01T00:00:00Z' \
         WHERE id = 'transition'",
    )
    .await
    .unwrap();
    let txn = db.begin().await.unwrap();
    assert!(
        insert_prepared_in_transaction(&txn, &claim, prepared.clone())
            .await
            .unwrap()
            .is_none()
    );
    txn.commit().await.unwrap();
    db.execute_unprepared(
        "UPDATE lifecycle_actions SET lease_until = '2999-01-01T00:00:00Z' \
         WHERE id = 'transition'",
    )
    .await
    .unwrap();
    assert!(
        lifecycle_transition::Entity::find()
            .all(&db)
            .await
            .unwrap()
            .is_empty(),
        "an invalid worker/epoch/lease fence must not create a saga"
    );

    let txn = db.begin().await.unwrap();
    let inserted = insert_prepared_in_transaction(&txn, &claim, prepared.clone())
        .await
        .unwrap()
        .unwrap();
    txn.commit().await.unwrap();
    let txn = db.begin().await.unwrap();
    let replayed = insert_prepared_in_transaction(&txn, &claim, prepared)
        .await
        .unwrap()
        .unwrap();
    txn.commit().await.unwrap();
    let after = database_now(&db).await.unwrap();

    assert_eq!(replayed, inserted);
    assert_eq!(inserted.action_id, "transition");
    assert_eq!(inserted.target_version_row_id, "version");
    assert_eq!(inserted.target_object_id, "object");
    assert_eq!(inserted.source_cid, "QmTransition");
    assert_eq!(inserted.destination_cid, "QmTransition");
    assert_eq!(inserted.source_tier, "hot");
    assert_eq!(inserted.destination_tier, "cold");
    assert_eq!(inserted.checkpoint, "prepare");
    assert!(inserted.created_at >= before && inserted.created_at <= after);
    assert_rejected(
        &db,
        "UPDATE lifecycle_transitions SET completed_at = CURRENT_TIMESTAMP \
         WHERE action_id = 'transition'",
    )
    .await;
    assert_rejected(
        &db,
        "UPDATE lifecycle_transitions SET checkpoint = 'cleanup', \
         verification_receipt = 'verify-receipt', publication_receipt = 'publish-receipt', \
         completed_at = CURRENT_TIMESTAMP WHERE action_id = 'transition'",
    )
    .await;
    let hold_txn = db.begin().await.unwrap();
    hold_txn
        .execute_unprepared(
            "UPDATE lifecycle_actions SET state = 'cancelled', lease_until = NULL, \
             claimed_by = NULL, finished_at = CURRENT_TIMESTAMP WHERE id = 'transition'",
        )
        .await
        .unwrap();
    hold_txn
        .execute_unprepared(
            "UPDATE lifecycle_transitions SET settlement_kind = 'cancelled', \
             completed_at = CURRENT_TIMESTAMP WHERE action_id = 'transition'",
        )
        .await
        .unwrap();
    hold_txn
        .execute_unprepared(&format!(
            "INSERT INTO residency_references \
             (owner_kind, owner_id, reason, version_row_id, object_id, tier, cid, created_at) \
             VALUES ('transition', '{}', 'transition_cleanup_hold', 'version', 'object', \
                     'hot', 'QmTransition', CURRENT_TIMESTAMP)",
            inserted.id
        ))
        .await
        .unwrap();
    assert!(
        store::lifecycle_transition::delete_settled_in_transaction(&hold_txn, &inserted.id)
            .await
            .is_err(),
        "settlement cleanup must not remove a saga that still owns residency holds"
    );
    hold_txn.rollback().await.unwrap();
    assert_rejected(
        &db,
        "UPDATE lifecycle_transitions SET target_sequence = 2 WHERE action_id = 'transition'",
    )
    .await;
    assert_rejected(
        &db,
        "UPDATE lifecycle_transitions SET verification_receipt = 'too-early' \
         WHERE action_id = 'transition'",
    )
    .await;
    assert_rejected(
        &db,
        "UPDATE lifecycle_transitions SET destination_tier = 'hot' \
         WHERE action_id = 'transition'",
    )
    .await;
    assert_rejected(
        &db,
        "UPDATE lifecycle_transitions SET expected_destination_node_identity = 'hot-node' \
         WHERE action_id = 'transition'",
    )
    .await;
    db.execute_unprepared("DELETE FROM object_versions WHERE id = 'version'")
        .await
        .unwrap();
    assert_eq!(
        lifecycle_transition::Entity::find_by_id(&inserted.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap(),
        inserted,
        "exact version deletion must preserve the durable saga snapshot"
    );
    db.execute_unprepared("DELETE FROM objects WHERE id = 'object'")
        .await
        .unwrap();
    assert_eq!(
        lifecycle_transition::Entity::find().all(&db).await.unwrap(),
        vec![inserted.clone()]
    );
    assert_rejected(
        &db,
        "DELETE FROM lifecycle_transitions WHERE action_id = 'transition'",
    )
    .await;
    assert_rejected(&db, "DELETE FROM lifecycle_actions WHERE id = 'transition'").await;
    assert_rejected(&db, "DELETE FROM buckets WHERE name = 'bucket'").await;
    assert!(store::bucket::delete(&db, "bucket").await.is_err());
    assert!(
        lifecycle_transition::Entity::find_by_id(&inserted.id)
            .one(&db)
            .await
            .unwrap()
            .is_some()
    );

    db.execute_unprepared(
        "UPDATE lifecycle_actions SET state = 'cancelled', lease_until = NULL, \
         claimed_by = NULL, finished_at = CURRENT_TIMESTAMP WHERE id = 'transition'",
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE lifecycle_transitions SET settlement_kind = 'cancelled', \
         completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP \
         WHERE action_id = 'transition'",
    )
    .await
    .unwrap();
    let txn = db.begin().await.unwrap();
    assert!(
        store::lifecycle_transition::delete_settled_in_transaction(&txn, &inserted.id)
            .await
            .unwrap()
    );
    txn.commit().await.unwrap();
    store::bucket::delete(&db, "bucket").await.unwrap();
    assert!(
        lifecycle_action::Entity::find_by_id("transition")
            .one(&db)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn published_unsettled_saga_blocks_all_cascades_until_cleanup_is_settled() {
    let db = sqlite().await;
    store::run_migrations(&db).await.unwrap();
    seed_bucket_and_content(&db).await;
    let identity = VersionResidencyIdentity::new("version", "object", "QmTransition");
    let txn = db.begin().await.unwrap();
    store::residency::attach_hot_in_transaction(
        &txn,
        &identity,
        &PhysicalVerification::verified("hot-node", "hot-receipt"),
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    db.execute_unprepared(
        "INSERT INTO lifecycle_actions \
         (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
          target_type, target_version_row_id, target_public_version_id, target_object_id, \
          target_sequence, due_at, state, next_attempt_at, claim_epoch, lease_until, claimed_by, \
          created_at, updated_at) VALUES \
         ('published-action', 'published-key', 'bucket', 'key', 4, 'id:transition', \
          'transition_current', 'version', 'version', 'version-public', 'object', 1, \
          CURRENT_TIMESTAMP, 'claimed', CURRENT_TIMESTAMP, 2, '2999-01-01T00:00:00Z', \
          'worker', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .await
    .unwrap();
    let action = lifecycle_action::Entity::find_by_id("published-action")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let claim = ClaimedLifecycleAction {
        action,
        claim_epoch: 2,
        worker_id: "worker".to_owned(),
    };
    let txn = db.begin().await.unwrap();
    let saga = insert_prepared_in_transaction(
        &txn,
        &claim,
        PreparedLifecycleTransition {
            source_residency_revision: 1,
            expected_source_node_identity: "hot-node".to_owned(),
            expected_destination_node_identity: "cold-node".to_owned(),
            ownership_generation: 8,
        },
    )
    .await
    .unwrap()
    .unwrap();
    txn.commit().await.unwrap();
    db.execute_unprepared(&format!(
        "UPDATE lifecycle_transitions SET checkpoint = 'publish', \
         verification_receipt = 'verify-receipt', publication_receipt = 'publish-receipt', \
         updated_at = CURRENT_TIMESTAMP WHERE id = '{}'",
        saga.id
    ))
    .await
    .unwrap();
    db.execute_unprepared("DELETE FROM object_versions WHERE id = 'version'")
        .await
        .unwrap();
    db.execute_unprepared("DELETE FROM objects WHERE id = 'object'")
        .await
        .unwrap();

    assert_rejected(
        &db,
        "DELETE FROM lifecycle_actions WHERE id = 'published-action'",
    )
    .await;
    assert_rejected(&db, "DELETE FROM buckets WHERE name = 'bucket'").await;
    assert_rejected(
        &db,
        "DELETE FROM lifecycle_transitions WHERE action_id = 'published-action'",
    )
    .await;
    assert!(store::bucket::delete(&db, "bucket").await.is_err());
    assert_eq!(
        lifecycle_transition::Entity::find_by_id(&saga.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .publication_receipt
            .as_deref(),
        Some("publish-receipt")
    );

    db.execute_unprepared(
        "UPDATE lifecycle_actions SET state = 'succeeded', lease_until = NULL, \
         claimed_by = NULL, finished_at = CURRENT_TIMESTAMP WHERE id = 'published-action'",
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE lifecycle_transitions SET checkpoint = 'cleanup', \
         settlement_kind = 'cleanup_complete', completed_at = CURRENT_TIMESTAMP, \
         updated_at = CURRENT_TIMESTAMP WHERE action_id = 'published-action'",
    )
    .await
    .unwrap();
    let txn = db.begin().await.unwrap();
    assert!(
        store::lifecycle_transition::delete_settled_in_transaction(&txn, &saga.id)
            .await
            .unwrap()
    );
    txn.commit().await.unwrap();
    store::bucket::delete(&db, "bucket").await.unwrap();
}
