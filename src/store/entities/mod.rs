pub mod bucket;
pub mod bucket_cors_config;
pub mod bucket_lifecycle_config;
pub mod import_destination;
pub mod import_job;
pub mod import_job_request;
pub mod import_job_result;
pub mod import_job_target;
pub mod import_prefix_claim;
pub mod lifecycle_action;
pub mod lifecycle_transition;
pub mod multipart_part;
pub mod multipart_upload;
pub mod object;
pub mod object_tag;
pub mod object_version;
pub mod physical_residency;
pub mod residency_backfill;
pub mod residency_reference;
pub mod standard_mutation_lease;
pub mod version_residency;
pub mod zip_batch;
pub mod zip_manifest_entry;
pub mod zip_root_build;
pub mod zip_root_reference;

#[cfg(test)]
mod tests {
    use super::object_tag;
    use sea_orm::{ConnectionTrait, Database, DatabaseConnection, EntityTrait, IntoActiveModel};

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
    async fn ordinary_object_tags_round_trip() {
        let db = setup().await;
        let tag = object_tag::Model {
            object_id: "object-1".to_owned(),
            key: "environment".to_owned(),
            value: "test".to_owned(),
        };
        object_tag::Entity::insert(tag.clone().into_active_model())
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
    }

    #[tokio::test]
    async fn import_constraints() {
        let db = setup().await;

        db.execute_unprepared(
            "INSERT INTO import_jobs \
             (id, bucket, key, source_type, request_fingerprint, \
              state, phase, attempts, next_attempt_at, claim_epoch, providers_observed, \
              pin_nodes_processed, pin_bytes_processed, downloaded_bytes, ipfs_add_bytes, \
              entries_processed, entries_succeeded, entries_failed, decompressed_bytes, created_at, updated_at) \
             VALUES ('job-1', 'bucket', 'key', 'cid', 'fingerprint-1', \
                     'queued', 'queued', 0, CURRENT_TIMESTAMP, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, \
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();

        db.execute_unprepared("INSERT INTO import_job_requests(job_id,source_value,metadata_json,tags_json) VALUES ('job-1','QmSource','{}','[]')").await.unwrap();

        for statement in [
            "INSERT INTO import_jobs \
             (id, bucket, key, source_type, request_fingerprint, \
              state, phase, attempts, next_attempt_at, claim_epoch, providers_observed, pin_nodes_processed, \
              pin_bytes_processed, downloaded_bytes, ipfs_add_bytes, entries_processed, entries_succeeded, \
              entries_failed, decompressed_bytes, created_at, updated_at) \
             VALUES ('invalid-source', 'bucket', 'key-2', 'invalid', 'fingerprint-2', \
                     'queued', 'queued', 0, CURRENT_TIMESTAMP, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, \
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            "INSERT INTO import_jobs \
             (id, bucket, key, source_type, request_fingerprint, \
              state, phase, attempts, next_attempt_at, claim_epoch, providers_observed, pin_nodes_processed, \
              pin_bytes_processed, downloaded_bytes, ipfs_add_bytes, entries_processed, entries_succeeded, \
              entries_failed, decompressed_bytes, created_at, updated_at) \
             VALUES ('invalid-state', 'bucket', 'key-3', 'cid', 'fingerprint-3', \
                     'invalid', 'queued', 0, CURRENT_TIMESTAMP, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, \
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            "INSERT INTO import_jobs \
             (id, bucket, key, source_type, request_fingerprint, \
              state, phase, attempts, next_attempt_at, claim_epoch, providers_observed, pin_nodes_processed, \
              pin_bytes_processed, downloaded_bytes, ipfs_add_bytes, entries_processed, entries_succeeded, \
              entries_failed, decompressed_bytes, created_at, updated_at) \
             VALUES ('invalid-phase', 'bucket', 'key-4', 'cid', 'fingerprint-4', \
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
             (id, bucket, key, source_type, request_fingerprint, \
              state, phase, attempts, next_attempt_at, claim_epoch, providers_observed, \
              pin_nodes_processed, pin_bytes_processed, downloaded_bytes, ipfs_add_bytes, \
              entries_processed, entries_succeeded, entries_failed, decompressed_bytes, created_at, updated_at) \
             VALUES ('job-null-token', 'bucket', 'key', 'cid', 'fingerprint-null-token', \
                     'queued', 'queued', 0, CURRENT_TIMESTAMP, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, \
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();

        db.execute_unprepared(
            "INSERT INTO import_jobs \
             (id, bucket, key, source_type, request_fingerprint, client_token, \
              state, phase, attempts, next_attempt_at, claim_epoch, providers_observed, \
              pin_nodes_processed, pin_bytes_processed, downloaded_bytes, ipfs_add_bytes, entries_processed, \
              entries_succeeded, entries_failed, decompressed_bytes, created_at, updated_at) \
             VALUES ('job-token', 'bucket', 'token-key', 'url', 'fingerprint-token', \
                     'token-1', 'queued', 'queued', 0, CURRENT_TIMESTAMP, 0, 0, 0, 0, 0, 0, 0, \
                     0, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();
        assert_rejected(
            &db,
            "INSERT INTO import_jobs \
             (id, bucket, key, source_type, request_fingerprint, client_token, \
              state, phase, attempts, next_attempt_at, claim_epoch, providers_observed, \
              pin_nodes_processed, pin_bytes_processed, downloaded_bytes, ipfs_add_bytes, entries_processed, \
              entries_succeeded, entries_failed, decompressed_bytes, created_at, updated_at) \
             VALUES ('job-token-duplicate', 'bucket', 'token-key', 'url', \
                     'fingerprint-token-duplicate', 'token-1', 'queued', 'queued', 0, \
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
            "import_job_requests",
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
             (id, bucket, key, source_type, request_fingerprint, \
              state, phase, attempts, next_attempt_at, claim_epoch, providers_observed, \
              pin_nodes_processed, pin_bytes_processed, downloaded_bytes, ipfs_add_bytes, \
              entries_processed, entries_succeeded, entries_failed, decompressed_bytes, created_at, updated_at, \
              completed_at) \
             VALUES ('job-retained', 'bucket', 'retained-key', 'cid', 'fingerprint-retained', \
                     'completed', 'publishing', 0, CURRENT_TIMESTAMP, 0, 0, 0, 0, 0, 0, 0, 0, \
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
