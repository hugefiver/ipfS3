use ipfs_s3_gateway::store::{initialization, run_migrations};
use sea_orm::{ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement};

#[path = "support/baseline_postgres.rs"]
mod postgres;

async fn upgrade(db: &DatabaseConnection) {
    let backend = db.get_database_backend();
    db.execute_unprepared("CREATE TABLE seaql_migrations(version TEXT PRIMARY KEY,applied_at BIGINT NOT NULL);
        INSERT INTO seaql_migrations VALUES ('m20261027_000001_schema_baseline',0),('m20261108_000001_current_pinning_config',0),('m20261109_000001_pin_cid_comparisons',0),('m20261110_000001_requeue_retained',0);
        CREATE TABLE object_versions(id TEXT PRIMARY KEY);
        INSERT INTO object_versions VALUES ('v1');
        CREATE TABLE object_tags(object_id TEXT, key TEXT, value TEXT);
        INSERT INTO object_tags VALUES ('file','env','production');
        CREATE TABLE import_job_requests(job_id TEXT PRIMARY KEY, metadata_json TEXT);
        INSERT INTO import_job_requests VALUES ('import','original');
        CREATE TABLE pin_management_revision(id TEXT);
        CREATE TABLE pin_jobs(id TEXT);
        INSERT INTO pin_jobs VALUES ('old-job');
        CREATE TABLE pin_custom(id TEXT);
        INSERT INTO pin_custom VALUES ('customer');").await.unwrap();
    match backend {
        DatabaseBackend::Sqlite => db.execute_unprepared("CREATE TRIGGER s75_import_job_requests_update AFTER UPDATE ON import_job_requests BEGIN INSERT INTO pin_management_revision VALUES (NEW.job_id); END;
            CREATE TRIGGER customer_import_guard BEFORE DELETE ON import_job_requests BEGIN SELECT RAISE(ABORT,'retain import'); END").await.unwrap(),
        DatabaseBackend::Postgres => db.execute_unprepared("CREATE FUNCTION s75_import_job_requests_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO pin_management_revision VALUES (NEW.job_id); RETURN NEW; END $$;
            CREATE TRIGGER s75_import_job_requests_update AFTER UPDATE ON import_job_requests FOR EACH ROW EXECUTE FUNCTION s75_import_job_requests_update();
            CREATE FUNCTION customer_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'retain import'; END $$;
            CREATE TRIGGER customer_import_guard BEFORE DELETE ON import_job_requests FOR EACH ROW EXECUTE FUNCTION customer_guard()").await.unwrap(),
        _ => unreachable!(),
    };
    assert!(run_migrations(db).await.is_err());
    // A name collision must roll back the removal of the legacy objects.
    db.execute_unprepared("CREATE TABLE pin_resources(customer_id TEXT)")
        .await
        .unwrap();
    assert!(initialization::upgrade_existing(db).await.is_err());
    let old = db
        .query_one(Statement::from_string(backend, "SELECT id FROM pin_jobs"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.try_get::<String>("", "id").unwrap(), "old-job");
    db.execute_unprepared("DROP TABLE pin_resources")
        .await
        .unwrap();
    initialization::upgrade_existing(db).await.unwrap();
    run_migrations(db).await.unwrap();
    initialization::upgrade_existing(db).await.unwrap();

    let row = db.query_one(Statement::from_string(backend, "SELECT object_versions.pin_decision, object_tags.value, import_job_requests.metadata_json, pin_custom.id AS custom_id FROM object_versions,object_tags,import_job_requests,pin_custom")).await.unwrap().unwrap();
    assert_eq!(
        row.try_get::<Option<String>>("", "pin_decision").unwrap(),
        None
    );
    assert_eq!(row.try_get::<String>("", "value").unwrap(), "production");
    assert_eq!(
        row.try_get::<String>("", "metadata_json").unwrap(),
        "original"
    );
    assert_eq!(row.try_get::<String>("", "custom_id").unwrap(), "customer");
    db.execute_unprepared("UPDATE import_job_requests SET metadata_json='updated'")
        .await
        .unwrap();
    assert!(
        db.execute_unprepared("DELETE FROM import_job_requests")
            .await
            .is_err()
    );
    for sql in [
        "SELECT id FROM pin_jobs",
        "SELECT id FROM pin_management_revision",
    ] {
        assert!(
            db.query_all(Statement::from_string(backend, sql))
                .await
                .is_err()
        );
    }
    for table in ["pin_resources", "pin_refs"] {
        let result = db
            .query_all(Statement::from_string(
                backend,
                format!("SELECT * FROM {table}"),
            ))
            .await
            .unwrap();
        assert!(result.is_empty());
    }
}

#[tokio::test]
async fn sqlite_upgrades_legacy_file_pin_storage() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    upgrade(&db).await;
}

#[tokio::test]
#[ignore = "requires isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_upgrades_legacy_file_pin_storage() {
    postgres::with_schema(|db| async move { upgrade(&db).await }).await;
}
