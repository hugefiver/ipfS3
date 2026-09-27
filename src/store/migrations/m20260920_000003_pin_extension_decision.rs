use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // No backfill: historical raw tags are not executable evidence. A foreign
        // key to the *internal* version row survives ambiguous public null IDs.
        manager.get_connection().execute_unprepared(
            "CREATE UNIQUE INDEX uq_object_versions_decision_owner ON object_versions(id, object_id)",
        ).await?;
        manager.get_connection().execute_unprepared(
            "CREATE TABLE pin_extension_decisions (\
                version_row_id TEXT PRIMARY KEY NOT NULL, \
                object_id TEXT NOT NULL REFERENCES objects(id) ON DELETE CASCADE, \
                control_revision TEXT NOT NULL UNIQUE, \
                config_revision TEXT NOT NULL, \
                effect TEXT NOT NULL CHECK (effect IN ('accepted', 'skipped', 'no_intent')), \
                snapshot TEXT NOT NULL, \
                CONSTRAINT ck_pin_extension_decisions_revision CHECK (length(config_revision) = 64), \
                CONSTRAINT fk_pin_extension_decisions_owner FOREIGN KEY (version_row_id, object_id) \
                    REFERENCES object_versions(id, object_id) ON DELETE CASCADE\
             )",
        ).await?;
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom(
            "extension decisions cannot be downgraded without losing historical intent evidence"
                .into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::{ConnectionTrait, Database, Statement};
    use sea_orm_migration::MigratorTrait;

    struct BeforeDecisions;

    impl MigratorTrait for BeforeDecisions {
        fn migrations() -> Vec<Box<dyn MigrationTrait>> {
            use crate::store::migrations::*;
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
                Box::new(m20260912_000002_lifecycle_transition::Migration),
                Box::new(m20260919_000001_standard_mutation_lease::Migration),
                Box::new(m20260920_000001_pin_submit_history::Migration),
                Box::new(m20260920_000002_pin_identity_ledger::Migration),
            ]
        }
    }

    #[tokio::test]
    async fn upgrade_old_db_keeps_raw_tag_but_does_not_backfill_executable_decision() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        BeforeDecisions::up(&db, None).await.unwrap();
        crate::store::bucket::create(&db, "bucket", None)
            .await
            .unwrap();
        crate::store::object::upsert(
            &db, "old", "bucket", "key", "bafy-old", 1, None, "bafy-old", None, false, None, None,
            false,
        )
        .await
        .unwrap();
        let old = crate::store::object::get_by_id(&db, "old").await.unwrap();
        crate::store::object_version::install_content_version(
            &db,
            crate::store::object_version::BucketVersioningState::Unversioned,
            &old,
            chrono::Utc::now(),
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "INSERT INTO object_tags (object_id, key, value) VALUES ('old', 'ipfs-s3:pin', 'true')",
        )
        .await
        .unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        let row = db
            .query_one(Statement::from_string(
                db.get_database_backend(),
                "SELECT COUNT(*) AS n FROM pin_extension_decisions".to_owned(),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.try_get::<i64>("", "n").unwrap(), 0);
        let version_row = db
            .query_one(Statement::from_string(
                db.get_database_backend(),
                "SELECT id FROM object_versions WHERE object_id = 'old'".to_owned(),
            ))
            .await
            .unwrap()
            .unwrap();
        let version_id: String = version_row.try_get("", "id").unwrap();
        assert!(
            crate::store::pinning::decision::read_for_version(&db, &version_id)
                .await
                .unwrap()
                .is_none()
        );
        crate::store::object::upsert(
            &db,
            "other",
            "bucket",
            "other",
            "bafy-other",
            1,
            None,
            "bafy-other",
            None,
            false,
            None,
            None,
            false,
        )
        .await
        .unwrap();
        // Both FKs would independently pass; the composite FK must refuse a
        // snapshot that claims a different object's exact version row.
        let wrong_owner = sea_orm::Statement::from_sql_and_values(
            db.get_database_backend(),
            "INSERT INTO pin_extension_decisions (version_row_id, object_id, control_revision, config_revision, effect, snapshot) VALUES (?, 'other', 'wrong-owner', ?, 'skipped', '{}')",
            vec![version_id.into(), "0".repeat(64).into()],
        );
        assert!(db.execute(wrong_owner).await.is_err());
        assert_eq!(
            crate::store::pinning::tags::list_object_tags(&db, "old")
                .await
                .unwrap()[0]
                .value,
            "true"
        );
    }
}
