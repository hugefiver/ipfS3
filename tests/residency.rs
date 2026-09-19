use std::collections::BTreeSet;

use chrono::Duration;
use ipfs_s3_gateway::{
    lifecycle::model::{LifecycleScanCursor, LifecycleScanSource},
    residency::{
        KuboTier, PhysicalVerification, ReferenceReason, ResidencyLocation, StorageClass,
        VerificationState, VersionResidencyIdentity,
    },
    store::{
        self,
        entities::{bucket_lifecycle_config, lifecycle_action},
        migrations::*,
        residency::*,
    },
};
use sea_orm::{
    ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, EntityTrait, QueryOrder,
    Statement, TransactionTrait,
};
use sea_orm_migration::{MigrationTrait, MigratorTrait, SchemaManager};

struct PreResidencyMigrator;

impl MigratorTrait for PreResidencyMigrator {
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
        ]
    }
}

async fn old_schema() -> DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .unwrap();
    PreResidencyMigrator::up(&db, None).await.unwrap();
    db
}

async fn seed_old_schema(db: &DatabaseConnection) {
    db.execute_unprepared("INSERT INTO buckets (name) VALUES ('bucket')")
        .await
        .unwrap();
    for statement in [
        "INSERT INTO objects \
         (id, bucket, key, cid, size, etag, encrypted, key_wrap, sse_c_key_fingerprint, \
          multipart, is_latest, created_at) \
         VALUES ('old-a', 'bucket', 'alpha', 'QmShared', 11, 'etag-old', TRUE, 'wrap-old', \
                 'fingerprint-old', FALSE, FALSE, '2026-09-01T00:00:00Z')",
        "INSERT INTO objects \
         (id, bucket, key, cid, size, etag, encrypted, key_wrap, multipart, is_latest, created_at) \
         VALUES ('current-a', 'bucket', 'alpha', 'QmCurrent', 12, 'etag-current', TRUE, \
                 'wrap-current', FALSE, TRUE, '2026-09-02T00:00:00Z')",
        "INSERT INTO objects \
         (id, bucket, key, cid, size, etag, multipart, is_latest, created_at) \
         VALUES ('shared-b', 'bucket', 'beta', 'QmShared', 11, 'etag-shared', FALSE, TRUE, \
                 '2026-09-03T00:00:00Z')",
        "INSERT INTO objects \
         (id, bucket, key, cid, size, etag, multipart, is_latest, created_at) \
         VALUES ('orphan', 'bucket', 'orphan', 'QmOrphan', 13, 'etag-orphan', FALSE, TRUE, \
                 '2026-09-04T00:00:00Z')",
    ] {
        db.execute_unprepared(statement).await.unwrap();
    }
    for statement in [
        "INSERT INTO object_versions \
         (id, bucket, key, version_id, kind, object_id, sequence, is_latest, \
          lifecycle_age_started_at, became_noncurrent_at, created_at, updated_at) \
         VALUES ('version-old', 'bucket', 'alpha', '00000000-0000-4000-8000-000000000001', \
                 'object', 'old-a', 1, FALSE, '2026-09-01T00:00:00Z', \
                 '2026-09-02T00:00:00Z', '2026-09-01T00:00:00Z', '2026-09-02T00:00:00Z')",
        "INSERT INTO object_versions \
         (id, bucket, key, version_id, kind, object_id, sequence, is_latest, \
          lifecycle_age_started_at, became_noncurrent_at, created_at, updated_at) \
         VALUES ('version-current', 'bucket', 'alpha', NULL, 'object', 'current-a', 2, TRUE, \
                 '2026-09-02T00:00:00Z', NULL, '2026-09-02T00:00:00Z', '2026-09-02T00:00:00Z')",
        "INSERT INTO object_versions \
         (id, bucket, key, version_id, kind, object_id, sequence, is_latest, \
          lifecycle_age_started_at, became_noncurrent_at, created_at, updated_at) \
         VALUES ('version-shared', 'bucket', 'beta', '00000000-0000-4000-8000-000000000002', \
                 'object', 'shared-b', 1, TRUE, '2026-09-03T00:00:00Z', NULL, \
                 '2026-09-03T00:00:00Z', '2026-09-03T00:00:00Z')",
        "INSERT INTO object_versions \
         (id, bucket, key, version_id, kind, object_id, sequence, is_latest, \
          lifecycle_age_started_at, became_noncurrent_at, created_at, updated_at) \
         VALUES ('marker', 'bucket', 'marker', '00000000-0000-4000-8000-000000000003', \
                 'delete_marker', NULL, 1, TRUE, '2026-09-04T00:00:00Z', NULL, \
                 '2026-09-04T00:00:00Z', '2026-09-04T00:00:00Z')",
    ] {
        db.execute_unprepared(statement).await.unwrap();
    }
    db.execute_unprepared(
        "INSERT INTO pin_leases \
         (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
          last_touched_at, expires_at, generation, state) \
         VALUES ('lease', 'shared-b', 'put_object', 'policy', 'all', 'full', CURRENT_TIMESTAMP, \
                 CURRENT_TIMESTAMP, '2099-01-01T00:00:00Z', 1, 'active')",
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "INSERT INTO pin_lease_targets \
         (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
         VALUES ('target', 'lease', 'QmShared', 11, 'pinata', 'pinned', \
                 CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .await
    .unwrap();
}

async fn seed_old_lifecycle_state(db: &DatabaseConnection) {
    db.execute_unprepared("INSERT INTO buckets (name) VALUES ('lifecycle-tombstone')")
        .await
        .unwrap();
    let cursor = store::lifecycle_scan::encode_cursor(&LifecycleScanCursor {
        source: LifecycleScanSource::Noncurrent,
        bucket: "bucket".to_owned(),
        key: "alpha".to_owned(),
        sequence: Some(1),
        version_row_id: Some("version-old".to_owned()),
        multipart_created_at: None,
        multipart_upload_id: None,
    });
    db.execute_unprepared(&format!(
        "INSERT INTO bucket_lifecycle_configs \
         (bucket, canonical_json, revision, scan_cursor, scan_lease_epoch, scan_lease_until, \
          created_at, updated_at, last_scanned_at) VALUES \
         ('bucket', '{{\"rules\":[]}}', 7, '{cursor}', 5, '2026-09-10T00:05:00Z', \
          '2026-09-01T00:00:00Z', '2026-09-10T00:00:00Z', '2026-09-09T00:00:00Z'), \
         ('lifecycle-tombstone', NULL, 13, NULL, 0, NULL, \
          '2026-08-01T00:00:00Z', '2026-09-11T00:00:00Z', NULL)"
    ))
    .await
    .unwrap();

    for statement in [
        "INSERT INTO lifecycle_actions \
         (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
          target_type, target_version_row_id, target_public_version_id, target_object_id, \
          target_sequence, target_upload_id, target_upload_created_at, due_at, state, attempts, \
          next_attempt_at, claim_epoch, lease_until, claimed_by, failure_class, \
          last_error_redacted, created_at, updated_at, finished_at) VALUES \
         ('phase-a-claimed', 'phase-a-claimed-key', 'bucket', 'alpha', 7, 'id:expire', \
          'expire_noncurrent', 'version', 'version-old', \
          '00000000-0000-4000-8000-000000000001', 'old-a', 1, NULL, NULL, \
          '2026-09-08T00:00:00Z', 'claimed', 3, '2026-09-08T00:01:00Z', 9, \
          '2026-09-08T00:05:00Z', 'worker-a', 'database_contention', \
          'lifecycle action failed', '2026-09-07T00:00:00Z', '2026-09-08T00:00:30Z', NULL)",
        "INSERT INTO lifecycle_actions \
         (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
          target_type, target_version_row_id, target_public_version_id, target_object_id, \
          target_sequence, target_upload_id, target_upload_created_at, due_at, state, attempts, \
          next_attempt_at, claim_epoch, lease_until, claimed_by, failure_class, \
          last_error_redacted, created_at, updated_at, finished_at) VALUES \
         ('phase-a-succeeded', 'phase-a-succeeded-key', 'bucket', 'alpha', 7, 'id:current', \
          'expire_current', 'version', 'version-current', 'null', 'current-a', 2, NULL, NULL, \
          '2026-09-06T00:00:00Z', 'succeeded', 1, '2026-09-06T00:00:00Z', 2, NULL, NULL, \
           NULL, NULL, '2026-09-05T00:00:00Z', '2026-09-06T00:00:30Z', \
           '2026-09-06T00:00:30Z')",
        "INSERT INTO lifecycle_actions \
         (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
          target_type, target_version_row_id, target_public_version_id, target_object_id, \
          target_sequence, target_upload_id, target_upload_created_at, due_at, state, attempts, \
          next_attempt_at, claim_epoch, lease_until, claimed_by, failure_class, \
          last_error_redacted, created_at, updated_at, finished_at) VALUES \
         ('phase-a-cancelled', 'phase-a-cancelled-key', 'bucket', 'marker', 7, 'id:marker', \
          'delete_expired_marker', 'version', 'marker', \
          '00000000-0000-4000-8000-000000000003', NULL, 1, NULL, NULL, \
          '2026-09-07T00:00:00Z', 'cancelled', 2, '2026-09-07T00:00:00Z', 3, NULL, NULL, \
          'cancelled_stale', 'lifecycle action failed', '2026-09-04T00:00:00Z', \
          '2026-09-07T00:00:30Z', '2026-09-07T00:00:30Z')",
        "INSERT INTO lifecycle_actions \
         (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
          target_type, target_version_row_id, target_public_version_id, target_object_id, \
          target_sequence, target_upload_id, target_upload_created_at, due_at, state, attempts, \
          next_attempt_at, claim_epoch, lease_until, claimed_by, failure_class, \
          last_error_redacted, created_at, updated_at, finished_at) VALUES \
         ('phase-b-claimed', 'phase-b-claimed-key', 'bucket', 'multipart', 7, 'id:abort', \
          'abort_incomplete_multipart_upload', 'multipart_upload', NULL, NULL, NULL, NULL, \
          'upload-claimed', '2026-09-01T00:00:00Z', '2026-09-08T00:00:00Z', 'claimed', 4, \
          '2026-09-08T00:02:00Z', 10, '2026-09-08T00:06:00Z', 'worker-b', \
          'internal_dependency', 'lifecycle action failed', '2026-09-01T00:00:00Z', \
          '2026-09-08T00:01:00Z', NULL)",
        "INSERT INTO lifecycle_actions \
         (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
          target_type, target_version_row_id, target_public_version_id, target_object_id, \
          target_sequence, target_upload_id, target_upload_created_at, due_at, state, attempts, \
          next_attempt_at, claim_epoch, lease_until, claimed_by, failure_class, \
          last_error_redacted, created_at, updated_at, finished_at) VALUES \
         ('phase-b-failed-safe', 'phase-b-failed-safe-key', 'bucket', 'multipart', 7, 'id:abort', \
          'abort_incomplete_multipart_upload', 'multipart_upload', NULL, NULL, NULL, NULL, \
          'upload-failed', '2026-08-01T00:00:00Z', '2026-09-04T00:00:00Z', 'failed_safe', 8, \
          '2026-09-04T00:00:00Z', 12, NULL, NULL, 'internal_dependency', \
          'lifecycle action failed', '2026-08-01T00:00:00Z', '2026-09-04T00:01:00Z', \
          '2026-09-04T00:01:00Z')",
    ] {
        db.execute_unprepared(statement).await.unwrap();
    }
}

async fn lifecycle_config_snapshot(db: &DatabaseConnection) -> Vec<bucket_lifecycle_config::Model> {
    bucket_lifecycle_config::Entity::find()
        .order_by_asc(bucket_lifecycle_config::Column::Bucket)
        .all(db)
        .await
        .unwrap()
}

async fn lifecycle_action_snapshot(db: &DatabaseConnection) -> Vec<lifecycle_action::Model> {
    lifecycle_action::Entity::find()
        .order_by_asc(lifecycle_action::Column::Id)
        .all(db)
        .await
        .unwrap()
}

async fn scalar_i64(db: &DatabaseConnection, sql: &str) -> i64 {
    db.query_one(Statement::from_string(DatabaseBackend::Sqlite, sql))
        .await
        .unwrap()
        .unwrap()
        .try_get_by(0)
        .unwrap()
}

#[tokio::test]
async fn quota_delayed_lease_targets_still_protect_only_hot_content() {
    let db = old_schema().await;
    seed_old_schema(&db).await;
    store::run_migrations(&db).await.unwrap();
    for state in ["quota_waiting", "quota_blocked"] {
        db.execute_unprepared(&format!(
            "UPDATE pin_lease_targets SET state = '{state}' WHERE id = 'target'"
        ))
        .await
        .unwrap();
        let txn = db.begin().await.unwrap();
        let hot = reference_summary_in_transaction(
            &txn,
            &ResidencyLocation::new(KuboTier::Hot, "QmShared"),
        )
        .await
        .unwrap();
        assert_eq!(hot.active_lease_targets, 1);
        assert_eq!(hot.active_lease_owners, 1);
        let cold = reference_summary_in_transaction(
            &txn,
            &ResidencyLocation::new(KuboTier::Cold, "QmShared"),
        )
        .await
        .unwrap();
        assert_eq!(cold.active_lease_targets, 0);
        assert_eq!(cold.active_lease_owners, 0);
        txn.commit().await.unwrap();
    }
}

async fn assert_rejected(db: &DatabaseConnection, sql: &str) {
    assert!(
        db.execute_unprepared(sql).await.is_err(),
        "SQLite accepted invalid residency state: {sql}"
    );
}

#[tokio::test]
async fn upgrade_backfills_only_live_content_versions_without_changing_identity() {
    let db = old_schema().await;
    seed_old_schema(&db).await;
    let before = db
        .query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT id, cid, etag, encrypted, key_wrap, sse_c_key_fingerprint, created_at \
             FROM objects ORDER BY id",
        ))
        .await
        .unwrap();

    store::run_migrations(&db).await.unwrap();

    assert_eq!(
        scalar_i64(&db, "SELECT COUNT(*) FROM version_residencies").await,
        3
    );
    assert_eq!(
        scalar_i64(&db, "SELECT COUNT(*) FROM residency_references").await,
        3
    );
    assert_eq!(
        scalar_i64(&db, "SELECT COUNT(*) FROM physical_residencies").await,
        2
    );
    assert_eq!(
        scalar_i64(
            &db,
            "SELECT COUNT(*) FROM version_residencies \
             WHERE primary_tier = 'hot' AND storage_class = 'STANDARD' AND revision = 1",
        )
        .await,
        3
    );
    assert_eq!(
        scalar_i64(
            &db,
            "SELECT COUNT(*) FROM physical_residencies WHERE verification_state = 'pending'",
        )
        .await,
        2
    );
    assert_eq!(
        scalar_i64(
            &db,
            "SELECT COUNT(*) FROM version_residencies WHERE version_row_id = 'marker' \
             OR object_id = 'orphan' OR cid = 'QmOrphan'",
        )
        .await,
        0,
        "markers and object rows with no surviving version must not be resurrected"
    );

    let after = db
        .query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT id, cid, etag, encrypted, key_wrap, sse_c_key_fingerprint, created_at \
             FROM objects ORDER BY id",
        ))
        .await
        .unwrap();
    assert_eq!(before.len(), after.len());
    for (before, after) in before.iter().zip(&after) {
        for column in [
            "id",
            "cid",
            "etag",
            "key_wrap",
            "sse_c_key_fingerprint",
            "created_at",
        ] {
            assert_eq!(
                before.try_get::<Option<String>>("", column).unwrap(),
                after.try_get::<Option<String>>("", column).unwrap(),
                "column {column} changed"
            );
        }
        assert_eq!(
            before.try_get::<bool>("", "encrypted").unwrap(),
            after.try_get::<bool>("", "encrypted").unwrap()
        );
    }

    let physical_cids = db
        .query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT cid FROM physical_residencies ORDER BY cid",
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String>("", "cid").unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        physical_cids,
        BTreeSet::from(["QmCurrent".to_owned(), "QmShared".to_owned()])
    );
}

#[tokio::test]
async fn upgrade_preserves_every_lifecycle_configuration_and_action_field() {
    let db = old_schema().await;
    seed_old_schema(&db).await;
    seed_old_lifecycle_state(&db).await;
    let configs_before = lifecycle_config_snapshot(&db).await;
    let actions_before = lifecycle_action_snapshot(&db).await;

    store::run_migrations(&db).await.unwrap();

    assert_eq!(lifecycle_config_snapshot(&db).await, configs_before);
    assert_eq!(lifecycle_action_snapshot(&db).await, actions_before);
}

#[tokio::test]
async fn schema_rejects_markers_invalid_states_and_invalid_owner_shapes() {
    let db = old_schema().await;
    seed_old_schema(&db).await;
    store::run_migrations(&db).await.unwrap();

    for sql in [
        "INSERT INTO physical_residencies \
         (tier, cid, verification_state, created_at, updated_at) \
         VALUES ('remote-provider', 'QmInvalidTier', 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        "INSERT INTO physical_residencies \
         (tier, cid, verification_state, created_at, updated_at) \
         VALUES ('hot', 'QmMissingProof', 'verified', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        "INSERT INTO version_residencies \
         (version_row_id, object_id, primary_tier, storage_class, cid, revision, created_at, updated_at) \
         VALUES ('marker', 'current-a', 'hot', 'STANDARD', 'QmCurrent', 1, \
                 CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        "UPDATE version_residencies SET cid = 'QmShared' \
         WHERE version_row_id = 'version-current'",
        "INSERT INTO residency_references \
         (owner_kind, owner_id, reason, version_row_id, object_id, tier, cid, created_at) \
         VALUES ('version', 'not-the-version', 'retained_version', 'version-current', \
                 'current-a', 'hot', 'QmCurrent', CURRENT_TIMESTAMP)",
        "INSERT INTO residency_references \
         (owner_kind, owner_id, reason, version_row_id, object_id, tier, cid, created_at) \
         VALUES ('transition', '', 'transition_staging', 'version-current', \
                 'current-a', 'hot', 'QmCurrent', CURRENT_TIMESTAMP)",
    ] {
        assert_rejected(&db, sql).await;
    }
}

#[tokio::test]
async fn attach_resolve_release_and_summary_preserve_shared_physical_state() {
    let db = old_schema().await;
    seed_old_schema(&db).await;
    store::run_migrations(&db).await.unwrap();
    let shared = VersionResidencyIdentity::new("version-old", "old-a", "QmShared");
    let location = ResidencyLocation::new(KuboTier::Hot, "QmShared");

    let txn = db.begin().await.unwrap();
    let resolved = attach_hot_in_transaction(
        &txn,
        &shared,
        &PhysicalVerification::verified("hot-node", "receipt-QmShared"),
    )
    .await
    .unwrap();
    assert_eq!(resolved.storage_class, StorageClass::Standard);
    assert_eq!(resolved.primary, location);
    assert_eq!(
        resolved.physical.verification_state,
        VerificationState::Verified
    );

    assert!(
        attach_transition_hold_in_transaction(
            &txn,
            "transition-1",
            ReferenceReason::TransitionStaging,
            &shared,
            &location,
        )
        .await
        .unwrap()
    );
    assert!(
        !attach_transition_hold_in_transaction(
            &txn,
            "transition-1",
            ReferenceReason::TransitionStaging,
            &shared,
            &location,
        )
        .await
        .unwrap(),
        "hold attach must be idempotent"
    );
    lock_residency_frontier(
        &txn,
        &["version-old".to_owned(), "version-shared".to_owned()],
        std::slice::from_ref(&location),
    )
    .await
    .unwrap();
    let before = reference_summary_in_transaction(&txn, &location)
        .await
        .unwrap();
    assert_eq!(before.retained_versions, 2);
    assert_eq!(before.transition_staging_holds, 1);
    assert_eq!(before.transition_cleanup_holds, 0);
    assert_eq!(before.active_lease_targets, 1);

    assert!(
        release_version_reference_in_transaction(&txn, "version-old")
            .await
            .unwrap()
    );
    assert!(
        !release_version_reference_in_transaction(&txn, "version-old")
            .await
            .unwrap(),
        "release must be idempotent"
    );
    let after = reference_summary_in_transaction(&txn, &location)
        .await
        .unwrap();
    assert_eq!(after.retained_versions, 1);
    assert!(after.has_known_references());
    txn.commit().await.unwrap();

    assert_eq!(
        scalar_i64(
            &db,
            "SELECT COUNT(*) FROM physical_residencies WHERE tier = 'hot' AND cid = 'QmShared'",
        )
        .await,
        1,
        "logical release must never remove physical residency"
    );
    assert_eq!(
        resolve_version_residency(&db, "version-shared")
            .await
            .unwrap()
            .physical
            .verification_state,
        VerificationState::Verified,
        "shared-CID verification is physical, not owned by one version"
    );
}

#[tokio::test]
async fn owner_validation_rejects_marker_mismatch_and_missing_version() {
    let db = old_schema().await;
    seed_old_schema(&db).await;
    store::run_migrations(&db).await.unwrap();

    for identity in [
        VersionResidencyIdentity::new("marker", "current-a", "QmCurrent"),
        VersionResidencyIdentity::new("version-current", "old-a", "QmShared"),
        VersionResidencyIdentity::new("missing", "current-a", "QmCurrent"),
    ] {
        let txn = db.begin().await.unwrap();
        assert!(
            attach_hot_in_transaction(&txn, &identity, &PhysicalVerification::Pending)
                .await
                .is_err()
        );
        txn.rollback().await.unwrap();
    }
}

#[tokio::test]
async fn backfill_claim_cursor_is_bounded_fenced_and_does_not_resurrect_deleted_versions() {
    let db = old_schema().await;
    seed_old_schema(&db).await;
    store::run_migrations(&db).await.unwrap();

    let claim = claim_residency_backfill(&db, "worker-a", Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.claim_epoch, 1);
    assert!(
        claim_residency_backfill(&db, "worker-b", Duration::seconds(30))
            .await
            .unwrap()
            .is_none()
    );
    let first = pending_hot_residency_page(&db, claim.cursor.as_ref(), 2)
        .await
        .unwrap();
    assert_eq!(first.items.len(), 2);
    assert!(!first.complete);
    assert!(first.next_cursor.is_some());

    let verify_txn = db.begin().await.unwrap();
    assert!(
        mark_hot_verified_in_transaction(
            &verify_txn,
            &VersionResidencyIdentity::new("version-current", "current-a", "QmCurrent"),
            "hot-node",
            "receipt-current",
        )
        .await
        .unwrap()
    );
    verify_txn.commit().await.unwrap();

    db.execute_unprepared("DELETE FROM object_versions WHERE id = 'version-shared'")
        .await
        .unwrap();
    let stale_verify_txn = db.begin().await.unwrap();
    assert!(
        mark_hot_verified_in_transaction(
            &stale_verify_txn,
            &VersionResidencyIdentity::new("version-shared", "shared-b", "QmShared"),
            "hot-node",
            "receipt-shared",
        )
        .await
        .is_err(),
        "verification writeback must revalidate the surviving immutable owner"
    );
    stale_verify_txn.rollback().await.unwrap();
    let all_remaining = pending_hot_residency_page(&db, None, 100).await.unwrap();
    assert!(
        all_remaining
            .items
            .iter()
            .all(|item| item.identity.version_row_id != "version-shared")
    );

    let txn = db.begin().await.unwrap();
    assert!(
        checkpoint_residency_backfill_in_transaction(
            &txn,
            &claim,
            first.next_cursor.as_ref(),
            false,
        )
        .await
        .unwrap()
    );
    txn.commit().await.unwrap();
    let next = claim_residency_backfill(&db, "worker-b", Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.claim_epoch, 2);
    assert_eq!(next.cursor, first.next_cursor);

    let stale_txn = db.begin().await.unwrap();
    assert!(
        !checkpoint_residency_backfill_in_transaction(
            &stale_txn,
            &claim,
            first.next_cursor.as_ref(),
            false,
        )
        .await
        .unwrap()
    );
    stale_txn.rollback().await.unwrap();
}

#[tokio::test]
async fn downgrade_is_allowed_only_for_reconstructable_pending_standard_state() {
    let pending_db = old_schema().await;
    seed_old_schema(&pending_db).await;
    store::run_migrations(&pending_db).await.unwrap();
    m20260912_000001_residency_references::Migration
        .down(&SchemaManager::new(&pending_db))
        .await
        .unwrap();

    for mutation in [
        "UPDATE version_residencies SET revision = 2 WHERE version_row_id = 'version-current'",
        "INSERT INTO physical_residencies \
         (tier, cid, verification_state, created_at, updated_at) \
         VALUES ('cold', 'QmPendingCold', 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        "UPDATE physical_residencies SET verification_state = 'verified', \
         node_identity = 'node', verification_receipt = 'receipt', verified_at = CURRENT_TIMESTAMP \
         WHERE tier = 'hot' AND cid = 'QmCurrent'",
        "INSERT INTO residency_references \
         (owner_kind, owner_id, reason, version_row_id, object_id, tier, cid, created_at) \
         VALUES ('transition', 'transition-1', 'transition_cleanup_hold', 'version-current', \
                 'current-a', 'hot', 'QmCurrent', CURRENT_TIMESTAMP)",
    ] {
        let db = old_schema().await;
        seed_old_schema(&db).await;
        store::run_migrations(&db).await.unwrap();
        db.execute_unprepared(mutation).await.unwrap();
        let result = m20260912_000001_residency_references::Migration
            .down(&SchemaManager::new(&db))
            .await;
        assert!(result.is_err());
        assert_eq!(
            scalar_i64(&db, "SELECT COUNT(*) FROM residency_backfill").await,
            1,
            "failed downgrade must preserve every residency table"
        );
    }

    let db = old_schema().await;
    seed_old_schema(&db).await;
    store::run_migrations(&db).await.unwrap();
    db.execute_unprepared(
        "INSERT INTO physical_residencies \
         (tier, cid, node_identity, verification_state, verification_receipt, verified_at, \
          created_at, updated_at) \
         VALUES ('cold', 'QmCurrent', 'cold-node', 'verified', 'cold-receipt', CURRENT_TIMESTAMP, \
                 CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE version_residencies SET primary_tier = 'cold', storage_class = 'STANDARD_IA', \
         revision = revision + 1 WHERE version_row_id = 'version-current'",
    )
    .await
    .unwrap();
    assert!(
        m20260912_000001_residency_references::Migration
            .down(&SchemaManager::new(&db))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn repeated_and_two_connection_upgrade_is_safe() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("residency-upgrade.db");
    let url = format!(
        "sqlite://{}?mode=rwc",
        path.display().to_string().replace('\\', "/")
    );
    let seed = store::connect_database(&url).await.unwrap();
    seed.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .unwrap();
    PreResidencyMigrator::up(&seed, None).await.unwrap();
    seed_old_schema(&seed).await;
    seed.close().await.unwrap();

    let first = store::connect_database(&url).await.unwrap();
    let second = store::connect_database(&url).await.unwrap();
    let (first_result, second_result) = tokio::join!(
        store::run_migrations(&first),
        store::run_migrations(&second)
    );
    first_result.unwrap();
    second_result.unwrap();
    store::run_migrations(&first).await.unwrap();

    assert_eq!(
        scalar_i64(&first, "SELECT COUNT(*) FROM version_residencies").await,
        3
    );
    assert_eq!(
        scalar_i64(
            &first,
            "SELECT COUNT(*) FROM seaql_migrations \
             WHERE version = 'm20260912_000001_residency_references'",
        )
        .await,
        1
    );
}

#[tokio::test]
async fn two_connections_claim_one_backfill_epoch_without_errors() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("residency-claim.db");
    let url = format!(
        "sqlite://{}?mode=rwc",
        path.display().to_string().replace('\\', "/")
    );
    let first = store::connect_database(&url).await.unwrap();
    store::run_migrations(&first).await.unwrap();
    let second = store::connect_database(&url).await.unwrap();

    let (left, right) = tokio::join!(
        claim_residency_backfill(&first, "worker-a", Duration::seconds(30)),
        claim_residency_backfill(&second, "worker-b", Duration::seconds(30)),
    );
    let claims = [left.unwrap(), right.unwrap()];
    assert_eq!(claims.iter().filter(|claim| claim.is_some()).count(), 1);
    assert_eq!(
        claims
            .iter()
            .flatten()
            .map(|claim| claim.claim_epoch)
            .collect::<Vec<_>>(),
        [1]
    );
}
