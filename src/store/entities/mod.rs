pub mod bucket;
pub mod bucket_cors_config;
pub mod bucket_lifecycle_config;
pub mod import_destination;
pub mod import_job;
pub mod import_job_result;
pub mod import_job_target;
pub mod import_prefix_claim;
pub mod lifecycle_action;
pub mod multipart_part;
pub mod multipart_upload;
pub mod object;
pub mod object_tag;
pub mod object_version;
pub mod pin_job;
pub mod pin_lease;
pub mod pin_lease_target;
pub mod pin_provider_usage;
pub mod remote_pin;

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use sea_orm::{ConnectionTrait, Database, DatabaseConnection, EntityTrait, IntoActiveModel};

    use super::{object_tag, pin_job, pin_lease, pin_lease_target, pin_provider_usage, remote_pin};

    fn timestamp(offset_seconds: i64) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 21, 0, 0, 0).single().unwrap()
            + Duration::seconds(offset_seconds)
    }

    async fn setup() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        db.execute_unprepared("INSERT INTO buckets (name) VALUES ('bucket')")
            .await
            .unwrap();
        db.execute_unprepared(
            "INSERT INTO objects (id, bucket, key, cid, size, etag) \
             VALUES ('object-1', 'bucket', 'key', 'QmObject', 7, 'QmObject')",
        )
        .await
        .unwrap();
        db
    }

    async fn assert_rejected(db: &DatabaseConnection, statement: &str) {
        assert!(
            db.execute_unprepared(statement).await.is_err(),
            "SQLite accepted invalid schema input: {statement}"
        );
    }

    #[tokio::test]
    async fn pinning_entities_round_trip_every_persisted_field() {
        let db = setup().await;
        let created_at = timestamp(0);
        let touched_at = timestamp(60);
        let expires_at = timestamp(3600);
        let retry_at = timestamp(120);
        let locked_until = timestamp(180);
        let updated_at = timestamp(240);

        let tag = object_tag::Model {
            object_id: "object-1".to_owned(),
            key: "environment".to_owned(),
            value: "test".to_owned(),
        };
        object_tag::Entity::insert(tag.clone().into_active_model())
            .exec(&db)
            .await
            .unwrap();

        let lease = pin_lease::Model {
            id: "lease-1".to_owned(),
            owner_object_id: "object-1".to_owned(),
            source: "put_object".to_owned(),
            policy_id: "policy-1".to_owned(),
            provider_mode: "all".to_owned(),
            content_mode: "full".to_owned(),
            created_at,
            last_touched_at: touched_at,
            expires_at,
            generation: 42,
            state: "active".to_owned(),
        };
        pin_lease::Entity::insert(lease.clone().into_active_model())
            .exec(&db)
            .await
            .unwrap();

        let target = pin_lease_target::Model {
            id: "target-1".to_owned(),
            lease_id: lease.id.clone(),
            cid: "QmTarget".to_owned(),
            logical_size: 1_024,
            provider: "pinata".to_owned(),
            state: "submitted".to_owned(),
            created_at,
            last_touched_at: touched_at,
        };
        pin_lease_target::Entity::insert(target.clone().into_active_model())
            .exec(&db)
            .await
            .unwrap();

        let remote = remote_pin::Model {
            provider: target.provider.clone(),
            cid: target.cid.clone(),
            request_id: Some("request-1".to_owned()),
            cid_size: target.logical_size,
            status: "pinning".to_owned(),
            epoch: 7,
            failure_attempts: 2,
            next_retry_at: Some(retry_at),
            last_failed_request_id: Some("request-0".to_owned()),
            last_touched_at: touched_at,
            last_error_class: Some("network".to_owned()),
            last_error_text: Some("timeout".to_owned()),
        };
        remote_pin::Entity::insert(remote.clone().into_active_model())
            .exec(&db)
            .await
            .unwrap();

        let job = pin_job::Model {
            id: "job-1".to_owned(),
            operation: "submit".to_owned(),
            provider: target.provider.clone(),
            cid: target.cid.clone(),
            lease_id: Some(lease.id.clone()),
            target_id: Some(target.id.clone()),
            expected_generation: Some(lease.generation),
            expected_remote_epoch: None,
            state: "running".to_owned(),
            attempts: 3,
            next_attempt_at: retry_at,
            locked_until: Some(locked_until),
            submit_phase: Some("recovering".to_owned()),
            last_error: Some("retrying remote submit".to_owned()),
            created_at,
            updated_at,
        };
        pin_job::Entity::insert(job.clone().into_active_model())
            .exec(&db)
            .await
            .unwrap();

        let unpin_job = pin_job::Model {
            id: "job-2".to_owned(),
            operation: "unpin".to_owned(),
            provider: target.provider.clone(),
            cid: target.cid.clone(),
            lease_id: None,
            target_id: None,
            expected_generation: None,
            expected_remote_epoch: Some(remote.epoch),
            state: "pending".to_owned(),
            attempts: 0,
            next_attempt_at: retry_at,
            locked_until: None,
            submit_phase: None,
            last_error: None,
            created_at,
            updated_at,
        };
        pin_job::Entity::insert(unpin_job.clone().into_active_model())
            .exec(&db)
            .await
            .unwrap();

        let usage = pin_provider_usage::Model {
            provider: target.provider.clone(),
            reserved_bytes: 4_096,
            reserved_pins: 5,
            observed_bytes: Some(8_192),
            observed_pins: Some(9),
            observed_at: Some(touched_at),
        };
        pin_provider_usage::Entity::insert(usage.clone().into_active_model())
            .exec(&db)
            .await
            .unwrap();

        assert_eq!(
            object_tag::Entity::find_by_id((tag.object_id.clone(), tag.key.clone()))
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            tag
        );
        assert_eq!(
            pin_lease::Entity::find_by_id(lease.id.clone())
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            lease
        );
        assert_eq!(
            pin_lease_target::Entity::find_by_id(target.id.clone())
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            target
        );
        assert_eq!(
            remote_pin::Entity::find_by_id((remote.provider.clone(), remote.cid.clone()))
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            remote
        );
        assert_eq!(
            pin_job::Entity::find_by_id(job.id.clone())
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            job
        );
        let loaded_unpin = pin_job::Entity::find_by_id(unpin_job.id.clone())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded_unpin.expected_remote_epoch, Some(remote.epoch));
        assert_eq!(loaded_unpin.submit_phase, None);
        assert_eq!(loaded_unpin, unpin_job);
        assert_eq!(
            pin_provider_usage::Entity::find_by_id(usage.provider.clone())
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            usage
        );
    }

    #[tokio::test]
    async fn sqlite_schema_rejects_invalid_states_and_job_scope_combinations() {
        let db = setup().await;
        db.execute_unprepared(
            "INSERT INTO pin_leases \
             (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
              last_touched_at, expires_at, generation, state) \
             VALUES ('lease-valid', 'object-1', 'source-valid', 'policy', 'all', 'full', \
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1, 'active')",
        )
        .await
        .unwrap();

        for statement in [
            "INSERT INTO pin_jobs \
             (id, operation, provider, cid, lease_id, target_id, state, attempts, next_attempt_at, submit_phase) \
             VALUES ('job-scope-1', 'submit', 'pinata', 'Qm', 'lease-valid', 'target-valid', \
                     'pending', 0, CURRENT_TIMESTAMP, 'ready')",
            "INSERT INTO pin_jobs \
             (id, operation, provider, cid, lease_id, target_id, expected_generation, expected_remote_epoch, \
              state, attempts, next_attempt_at, submit_phase) \
             VALUES ('job-scope-2', 'submit', 'pinata', 'Qm', 'lease-valid', 'target-valid', 1, 1, \
                     'pending', 0, CURRENT_TIMESTAMP, 'ready')",
            "INSERT INTO pin_jobs (id, operation, provider, cid, state, attempts, next_attempt_at, submit_phase) \
             VALUES ('job-scope-3', 'unpin', 'pinata', 'Qm', 'pending', 0, CURRENT_TIMESTAMP, NULL)",
            "INSERT INTO pin_jobs \
             (id, operation, provider, cid, lease_id, target_id, expected_generation, expected_remote_epoch, \
              state, attempts, next_attempt_at, submit_phase) \
             VALUES ('job-scope-4', 'unpin', 'pinata', 'Qm', 'lease-valid', 'target-valid', 1, 1, \
                     'pending', 0, CURRENT_TIMESTAMP, NULL)",
            "INSERT INTO pin_jobs \
             (id, operation, provider, cid, lease_id, target_id, expected_generation, state, attempts, \
              next_attempt_at, submit_phase) \
             VALUES ('job-no-phase', 'submit', 'pinata', 'Qm', 'lease-valid', 'target-valid', 1, \
                     'pending', 0, CURRENT_TIMESTAMP, NULL)",
            "INSERT INTO pin_jobs \
             (id, operation, provider, cid, expected_remote_epoch, state, attempts, next_attempt_at, submit_phase) \
             VALUES ('job-non-submit-phase', 'unpin', 'pinata', 'Qm', 1, 'pending', 0, \
                     CURRENT_TIMESTAMP, 'ready')",
            "INSERT INTO pin_jobs \
             (id, operation, provider, cid, expected_remote_epoch, state, attempts, next_attempt_at, submit_phase) \
             VALUES ('job-operation', 'unexpected', 'pinata', 'Qm', 1, 'pending', 0, \
                     CURRENT_TIMESTAMP, NULL)",
            "INSERT INTO pin_leases \
             (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
              last_touched_at, expires_at, generation, state) \
             VALUES ('lease-invalid', 'object-1', 'source-invalid', 'policy', 'all', 'full', \
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1, 'invalid')",
            "INSERT INTO pin_lease_targets \
             (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
             VALUES ('target-invalid', 'lease-valid', 'Qm', 1, 'pinata', 'invalid', \
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            "INSERT INTO remote_pins \
             (provider, cid, cid_size, status, epoch, failure_attempts, last_touched_at) \
             VALUES ('pinata', 'Qm-invalid-state', 1, 'invalid', 1, 0, CURRENT_TIMESTAMP)",
            "INSERT INTO remote_pins \
             (provider, cid, cid_size, status, epoch, failure_attempts, last_touched_at) \
             VALUES ('pinata', 'Qm-negative-failure', 1, 'reserved', 1, -1, CURRENT_TIMESTAMP)",
            "INSERT INTO remote_pins \
             (provider, cid, cid_size, status, epoch, failure_attempts, last_touched_at) \
             VALUES ('pinata', 'Qm-invalid-epoch', 1, 'reserved', 0, 0, CURRENT_TIMESTAMP)",
            "INSERT INTO pin_jobs \
             (id, operation, provider, cid, lease_id, target_id, expected_generation, state, attempts, \
              next_attempt_at, submit_phase) \
             VALUES ('job-invalid-state', 'submit', 'pinata', 'Qm', 'lease-valid', 'target-valid', 1, \
                     'invalid', 0, CURRENT_TIMESTAMP, 'ready')",
            "INSERT INTO pin_jobs \
             (id, operation, provider, cid, lease_id, target_id, expected_generation, state, attempts, \
              next_attempt_at, submit_phase) \
             VALUES ('job-invalid-phase', 'submit', 'pinata', 'Qm', 'lease-valid', 'target-valid', 1, \
                     'pending', 0, CURRENT_TIMESTAMP, 'invalid')",
        ] {
            assert_rejected(&db, statement).await;
        }
    }

    #[tokio::test]
    async fn sqlite_schema_defaults_remote_retry_fields_and_submit_phase() {
        let db = setup().await;
        assert_rejected(
            &db,
            "INSERT INTO remote_pins (provider, cid, status, epoch, last_touched_at) \
             VALUES ('pinata', 'Qm-missing-size', 'reserved', 1, CURRENT_TIMESTAMP)",
        )
        .await;
        db.execute_unprepared(
            "INSERT INTO remote_pins (provider, cid, cid_size, status, epoch, last_touched_at) \
             VALUES ('pinata', 'Qm-defaults', 11, 'reserved', 1, CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "INSERT INTO pin_jobs \
             (id, operation, provider, cid, lease_id, target_id, expected_generation, next_attempt_at) \
             VALUES ('job-defaults', 'submit', 'pinata', 'Qm-defaults', 'lease-1', 'target-1', 1, \
                     CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();

        let remote =
            remote_pin::Entity::find_by_id(("pinata".to_owned(), "Qm-defaults".to_owned()))
                .one(&db)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(remote.failure_attempts, 0);
        assert_eq!(remote.cid_size, 11);
        assert_eq!(remote.next_retry_at, None);
        assert_eq!(remote.last_failed_request_id, None);
        assert_eq!(remote.last_error_class, None);
        assert_eq!(remote.last_error_text, None);

        let job = pin_job::Entity::find_by_id("job-defaults".to_owned())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.submit_phase.as_deref(), Some("ready"));
        assert_eq!(job.state, "pending");
        assert_eq!(job.attempts, 0);
        assert_eq!(job.locked_until, None);
        assert_eq!(job.last_error, None);
    }

    #[tokio::test]
    async fn import_constraints() {
        let db = setup().await;

        db.execute_unprepared(
            "INSERT INTO import_jobs \
             (id, bucket, key, source_type, source_value, request_fingerprint, metadata_json, \
              tags_json, state, phase, attempts, next_attempt_at, claim_epoch, providers_observed, \
              pin_nodes_processed, pin_bytes_processed, downloaded_bytes, ipfs_add_bytes, \
              entries_processed, entries_succeeded, entries_failed, decompressed_bytes, created_at, updated_at) \
             VALUES ('job-1', 'bucket', 'key', 'cid', 'QmSource', 'fingerprint-1', '{}', '[]', \
                     'queued', 'queued', 0, CURRENT_TIMESTAMP, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, \
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();

        for statement in [
            "INSERT INTO import_jobs \
             (id, bucket, key, source_type, source_value, request_fingerprint, metadata_json, tags_json, \
              state, phase, attempts, next_attempt_at, claim_epoch, providers_observed, pin_nodes_processed, \
              pin_bytes_processed, downloaded_bytes, ipfs_add_bytes, entries_processed, entries_succeeded, \
              entries_failed, decompressed_bytes, created_at, updated_at) \
             VALUES ('invalid-source', 'bucket', 'key-2', 'invalid', 'source', 'fingerprint-2', '{}', '[]', \
                     'queued', 'queued', 0, CURRENT_TIMESTAMP, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, \
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            "INSERT INTO import_jobs \
             (id, bucket, key, source_type, source_value, request_fingerprint, metadata_json, tags_json, \
              state, phase, attempts, next_attempt_at, claim_epoch, providers_observed, pin_nodes_processed, \
              pin_bytes_processed, downloaded_bytes, ipfs_add_bytes, entries_processed, entries_succeeded, \
              entries_failed, decompressed_bytes, created_at, updated_at) \
             VALUES ('invalid-state', 'bucket', 'key-3', 'cid', 'source', 'fingerprint-3', '{}', '[]', \
                     'invalid', 'queued', 0, CURRENT_TIMESTAMP, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, \
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            "INSERT INTO import_jobs \
             (id, bucket, key, source_type, source_value, request_fingerprint, metadata_json, tags_json, \
              state, phase, attempts, next_attempt_at, claim_epoch, providers_observed, pin_nodes_processed, \
              pin_bytes_processed, downloaded_bytes, ipfs_add_bytes, entries_processed, entries_succeeded, \
              entries_failed, decompressed_bytes, created_at, updated_at) \
             VALUES ('invalid-phase', 'bucket', 'key-4', 'cid', 'source', 'fingerprint-4', '{}', '[]', \
                     'queued', 'invalid', 0, CURRENT_TIMESTAMP, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, \
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        ] {
            assert_rejected(&db, statement).await;
        }
        for column in [
            "attempts",
            "claim_epoch",
            "providers_observed",
            "pin_nodes_processed",
            "pin_bytes_processed",
            "downloaded_bytes",
            "download_total",
            "ipfs_add_bytes",
            "logical_size",
            "entries_processed",
            "entries_succeeded",
            "entries_failed",
            "decompressed_bytes",
        ] {
            assert_rejected(
                &db,
                &format!("UPDATE import_jobs SET {column} = -1 WHERE id = 'job-1'"),
            )
            .await;
        }

        db.execute_unprepared(
            "INSERT INTO import_jobs \
             (id, bucket, key, source_type, source_value, request_fingerprint, metadata_json, \
              tags_json, state, phase, attempts, next_attempt_at, claim_epoch, providers_observed, \
              pin_nodes_processed, pin_bytes_processed, downloaded_bytes, ipfs_add_bytes, \
              entries_processed, entries_succeeded, entries_failed, decompressed_bytes, created_at, updated_at) \
             VALUES ('job-null-token', 'bucket', 'key', 'cid', 'QmSource', 'fingerprint-null-token', '{}', '[]', \
                     'queued', 'queued', 0, CURRENT_TIMESTAMP, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, \
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();

        db.execute_unprepared(
            "INSERT INTO import_jobs \
             (id, bucket, key, source_type, source_value, request_fingerprint, client_token, metadata_json, \
              tags_json, state, phase, attempts, next_attempt_at, claim_epoch, providers_observed, \
              pin_nodes_processed, pin_bytes_processed, downloaded_bytes, ipfs_add_bytes, entries_processed, \
              entries_succeeded, entries_failed, decompressed_bytes, created_at, updated_at) \
             VALUES ('job-token', 'bucket', 'token-key', 'url', 'https://example.test/object', 'fingerprint-token', \
                     'token-1', '{}', '[]', 'queued', 'queued', 0, CURRENT_TIMESTAMP, 0, 0, 0, 0, 0, 0, 0, \
                     0, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();
        assert_rejected(
            &db,
            "INSERT INTO import_jobs \
             (id, bucket, key, source_type, source_value, request_fingerprint, client_token, metadata_json, \
              tags_json, state, phase, attempts, next_attempt_at, claim_epoch, providers_observed, \
              pin_nodes_processed, pin_bytes_processed, downloaded_bytes, ipfs_add_bytes, entries_processed, \
              entries_succeeded, entries_failed, decompressed_bytes, created_at, updated_at) \
             VALUES ('job-token-duplicate', 'bucket', 'token-key', 'url', 'https://example.test/object', \
                     'fingerprint-token-duplicate', 'token-1', '{}', '[]', 'queued', 'queued', 0, \
                     CURRENT_TIMESTAMP, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await;

        assert_rejected(
            &db,
            "INSERT INTO import_destinations (bucket, key, generation, updated_at) \
             VALUES ('bucket', 'invalid-destination', 0, CURRENT_TIMESTAMP)",
        )
        .await;
        db.execute_unprepared(
            "INSERT INTO import_destinations (bucket, key, generation, owner_job_id, updated_at) \
             VALUES ('bucket', 'key', 1, 'job-1', CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "INSERT INTO import_prefix_claims (job_id, bucket, prefix, claim_order) \
             VALUES ('job-1', 'bucket', 'prefix/', 0)",
        )
        .await
        .unwrap();
        assert_rejected(
            &db,
            "INSERT INTO import_prefix_claims (job_id, bucket, prefix, claim_order) \
             VALUES ('job-1', 'bucket', 'negative-prefix/', -1)",
        )
        .await;
        assert_rejected(
            &db,
            "INSERT INTO import_job_targets (job_id, bucket, key, expected_generation, kind) \
             VALUES ('job-1', 'bucket', 'invalid-generation', 0, 'archive')",
        )
        .await;
        assert_rejected(
            &db,
            "INSERT INTO import_job_targets (job_id, bucket, key, expected_generation, kind) \
             VALUES ('job-1', 'bucket', 'invalid-kind', 1, 'invalid')",
        )
        .await;
        db.execute_unprepared(
            "INSERT INTO import_job_targets (job_id, bucket, key, expected_generation, kind) \
             VALUES ('job-1', 'bucket', 'key', 1, 'archive')",
        )
        .await
        .unwrap();
        assert_rejected(
            &db,
            "INSERT INTO import_job_targets (job_id, bucket, key, expected_generation, kind) \
             VALUES ('job-1', 'bucket', 'key', 1, 'archive')",
        )
        .await;
        db.execute_unprepared(
            "INSERT INTO import_job_results (job_id, sequence, key, cid, size) \
             VALUES ('job-1', 0, 'key', 'QmResult', 7)",
        )
        .await
        .unwrap();
        assert_rejected(
            &db,
            "INSERT INTO import_job_results (job_id, sequence, key, cid, size) \
             VALUES ('job-1', 0, 'key-duplicate', 'QmResult', 7)",
        )
        .await;
        db.execute_unprepared(
            "INSERT INTO import_job_results (job_id, sequence, key, error_code, error_message) \
             VALUES ('job-1', 1, 'failed-key', 'invalid_archive', 'archive is malformed')",
        )
        .await
        .unwrap();
        for statement in [
            "INSERT INTO import_job_results (job_id, sequence, key, cid, size, error_code) \
             VALUES ('job-1', 2, 'mixed', 'QmResult', 7, 'mixed')",
            "INSERT INTO import_job_results (job_id, sequence, key) \
             VALUES ('job-1', 3, 'incomplete')",
            "INSERT INTO import_job_results (job_id, sequence, key, error_code) \
             VALUES ('job-1', 4, 'incomplete-failure', 'failed')",
        ] {
            assert_rejected(&db, statement).await;
        }
        for column in ["sequence", "size"] {
            assert_rejected(
                &db,
                &format!("UPDATE import_job_results SET {column} = -1 WHERE job_id = 'job-1'"),
            )
            .await;
        }

        db.execute_unprepared("UPDATE import_jobs SET state = 'completed' WHERE id = 'job-1'")
            .await
            .unwrap();
        db.execute_unprepared("DELETE FROM import_jobs WHERE id = 'job-1'")
            .await
            .unwrap();
        for table in [
            "import_prefix_claims",
            "import_job_targets",
            "import_job_results",
        ] {
            let count: i64 = db
                .query_one(sea_orm::Statement::from_string(
                    sea_orm::DatabaseBackend::Sqlite,
                    format!("SELECT COUNT(*) FROM {table}"),
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get_by(0)
                .unwrap();
            assert_eq!(count, 0, "{table} must cascade with its job");
        }
        let destination_count: i64 = db
            .query_one(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT COUNT(*) FROM import_destinations WHERE bucket = 'bucket' AND key = 'key' AND generation = 1",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get_by(0)
            .unwrap();
        assert_eq!(
            destination_count, 1,
            "destination generation must survive job deletion"
        );
        let destination = db
            .query_one(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT owner_job_id FROM import_destinations WHERE bucket = 'bucket' AND key = 'key'",
            ))
            .await
            .unwrap()
            .unwrap();
        let owner_job_id: Option<String> = destination.try_get_by(0).unwrap();
        assert_eq!(
            owner_job_id, None,
            "destination ownership must clear with its job"
        );

        db.execute_unprepared(
            "INSERT INTO import_jobs \
             (id, bucket, key, source_type, source_value, request_fingerprint, metadata_json, \
              tags_json, state, phase, attempts, next_attempt_at, claim_epoch, providers_observed, \
              pin_nodes_processed, pin_bytes_processed, downloaded_bytes, ipfs_add_bytes, \
              entries_processed, entries_succeeded, entries_failed, decompressed_bytes, created_at, updated_at, \
              completed_at) \
             VALUES ('job-retained', 'bucket', 'retained-key', 'cid', 'QmRetained', 'fingerprint-retained', \
                     '{}', '[]', 'completed', 'publishing', 0, CURRENT_TIMESTAMP, 0, 0, 0, 0, 0, 0, 0, 0, \
                     0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "INSERT INTO import_destinations (bucket, key, generation, owner_job_id, updated_at) \
             VALUES ('bucket', 'retained-key', 1, 'job-retained', CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "INSERT INTO import_prefix_claims (job_id, bucket, prefix, claim_order) \
             VALUES ('job-retained', 'bucket', 'retained-prefix/', 0)",
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "INSERT INTO import_job_targets (job_id, bucket, key, expected_generation, kind) \
             VALUES ('job-retained', 'bucket', 'retained-key', 1, 'archive')",
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "INSERT INTO import_job_results (job_id, sequence, key, cid, size) \
             VALUES ('job-retained', 0, 'retained-key', 'QmRetained', 7)",
        )
        .await
        .unwrap();

        db.execute_unprepared("DELETE FROM buckets WHERE name = 'bucket'")
            .await
            .unwrap();
        for table in [
            "import_destinations",
            "import_prefix_claims",
            "import_job_targets",
        ] {
            let count: i64 = db
                .query_one(sea_orm::Statement::from_string(
                    sea_orm::DatabaseBackend::Sqlite,
                    format!("SELECT COUNT(*) FROM {table} WHERE bucket = 'bucket'"),
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get_by(0)
                .unwrap();
            assert_eq!(count, 0, "{table} must cascade with its bucket");
        }
        for (table, job_id_column) in [("import_jobs", "id"), ("import_job_results", "job_id")] {
            let count: i64 = db
                .query_one(sea_orm::Statement::from_string(
                    sea_orm::DatabaseBackend::Sqlite,
                    format!("SELECT COUNT(*) FROM {table} WHERE {job_id_column} = 'job-retained'"),
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get_by(0)
                .unwrap();
            assert_eq!(count, 1, "{table} must survive bucket deletion");
        }
    }
}
