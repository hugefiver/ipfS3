pub mod bucket;
pub mod multipart_part;
pub mod multipart_upload;
pub mod object;
pub mod object_tag;
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
}
