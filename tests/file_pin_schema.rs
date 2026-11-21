use ipfs_s3_gateway::store::{initialization, run_migrations};
use sea_orm::{ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement};

#[path = "support/baseline_postgres.rs"]
mod postgres;

async fn upgrade(db: &DatabaseConnection) {
    let backend = db.get_database_backend();
    db.execute_unprepared("CREATE TABLE seaql_migrations(version TEXT PRIMARY KEY,applied_at BIGINT NOT NULL);
        INSERT INTO seaql_migrations VALUES ('m20261027_000001_schema_baseline',0),('m20261108_000001_current_pinning_config',0),('m20261109_000001_pin_cid_comparisons',0),('m20261110_000001_requeue_retained',0);
        CREATE TABLE buckets(name TEXT PRIMARY KEY, owner TEXT);
        INSERT INTO buckets VALUES ('legacy','owner');
        CREATE TABLE objects(id TEXT PRIMARY KEY, size BIGINT NOT NULL);
        INSERT INTO objects VALUES ('file',3);
        CREATE TABLE object_versions(id TEXT PRIMARY KEY, bucket TEXT NOT NULL, kind TEXT NOT NULL, object_id TEXT);
        INSERT INTO object_versions VALUES ('v1','legacy','object','file');
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
    let bucket = db
        .query_one(Statement::from_string(
            backend,
            "SELECT owner,canned_acl,used_bytes FROM buckets WHERE name='legacy'",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bucket.try_get::<String>("", "owner").unwrap(), "owner");
    assert_eq!(bucket.try_get::<i64>("", "used_bytes").unwrap(), 3);
    assert_eq!(
        bucket.try_get::<Option<String>>("", "canned_acl").unwrap(),
        None
    );
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

async fn upgrade_file_pin_bucket_acl(db: &DatabaseConnection) {
    run_migrations(db).await.unwrap();
    db.execute_unprepared("ALTER TABLE buckets DROP COLUMN quota_bytes;
        ALTER TABLE buckets DROP COLUMN used_bytes;
        ALTER TABLE buckets DROP COLUMN canned_acl;
        UPDATE seaql_migrations SET version='m20261120_000001_file_pins';
        INSERT INTO buckets(name, owner) VALUES ('photos','alice');
        INSERT INTO multipart_uploads(upload_id,object_id,bucket,key) VALUES ('upload','pending','photos','video');
        INSERT INTO multipart_parts(upload_id,part_number,cid,size,etag) VALUES ('upload',1,'part-cid',5,'part-etag');
        INSERT INTO pin_resources(provider,cid,logical_size,status,handle) VALUES ('remote','pinned-cid',5,'accepted','handle');
        INSERT INTO pin_refs(owner_object_id,provider,cid,source,policy_id,content_mode,expires_at) VALUES ('file','remote','pinned-cid','manual','policy','object',99)").await.unwrap();
    assert!(run_migrations(db).await.is_err());
    initialization::upgrade_existing(db).await.unwrap();
    run_migrations(db).await.unwrap();
    let row = db.query_one(Statement::from_string(db.get_database_backend(),
        "SELECT b.owner,b.canned_acl,b.used_bytes,p.cid AS part,r.handle FROM buckets b
         JOIN multipart_uploads u ON b.name=u.bucket JOIN multipart_parts p ON u.upload_id=p.upload_id,
         pin_refs f JOIN pin_resources r ON f.provider=r.provider AND f.cid=r.cid
         WHERE b.name='photos' AND f.owner_object_id='file'" )).await.unwrap().unwrap();
    assert_eq!(row.try_get::<String>("", "owner").unwrap(), "alice");
    assert_eq!(
        row.try_get::<Option<String>>("", "canned_acl").unwrap(),
        None
    );
    assert_eq!(row.try_get::<i64>("", "used_bytes").unwrap(), 0);
    assert_eq!(row.try_get::<String>("", "part").unwrap(), "part-cid");
    assert_eq!(row.try_get::<String>("", "handle").unwrap(), "handle");
    assert!(
        db.execute_unprepared("UPDATE buckets SET canned_acl='unknown'")
            .await
            .is_err()
    );
}

async fn upgrade_bucket_quota(db: &DatabaseConnection) {
    run_migrations(db).await.unwrap();
    db.execute_unprepared("ALTER TABLE buckets DROP COLUMN quota_bytes;
        ALTER TABLE buckets DROP COLUMN used_bytes;
        UPDATE seaql_migrations SET version='m20261121_000001_bucket_acl';
        INSERT INTO buckets(name,canned_acl) VALUES ('files','public-read'),('empty',NULL);
        INSERT INTO objects(id,bucket,key,cid,size,etag,is_latest) VALUES
            ('current','files','live','shared-cid',11,'shared-cid',TRUE),
            ('null-slot','files','live','shared-cid',11,'shared-cid',FALSE),
            ('masked','files','hidden','masked-cid',5000000000,'masked-cid',FALSE),
            ('unlinked','files','unlinked','unlinked-cid',100,'unlinked-cid',TRUE);
        INSERT INTO object_versions(id,bucket,key,version_id,kind,object_id,sequence,is_latest,lifecycle_age_started_at,created_at,updated_at) VALUES
            ('current-version','files','live','opaque','object','current',2,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
            ('null-version','files','live',NULL,'object','null-slot',1,FALSE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
            ('masked-version','files','hidden','old','object','masked',1,FALSE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
            ('marker','files','hidden','marker','delete_marker',NULL,2,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    assert!(run_migrations(db).await.is_err());
    initialization::upgrade_existing(db).await.unwrap();
    run_migrations(db).await.unwrap();

    let rows = db
        .query_all(Statement::from_string(
            db.get_database_backend(),
            "SELECT name,canned_acl,quota_bytes,used_bytes FROM buckets ORDER BY name",
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    for (row, (name, acl, used)) in rows.iter().zip([
        ("empty", None, 0),
        ("files", Some("public-read"), 5_000_000_022),
    ]) {
        assert_eq!(row.try_get::<String>("", "name").unwrap(), name);
        assert_eq!(
            row.try_get::<Option<String>>("", "canned_acl")
                .unwrap()
                .as_deref(),
            acl
        );
        assert_eq!(row.try_get::<Option<i64>>("", "quota_bytes").unwrap(), None);
        assert_eq!(row.try_get::<i64>("", "used_bytes").unwrap(), used);
    }
}

#[tokio::test]
async fn sqlite_upgrade_backfills_bucket_usage_for_addressable_versions() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    upgrade_bucket_quota(&db).await;
}

#[tokio::test]
#[ignore = "requires isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_upgrade_backfills_bucket_usage_for_addressable_versions() {
    postgres::with_schema(|db| async move { upgrade_bucket_quota(&db).await }).await;
}

#[tokio::test]
async fn sqlite_upgrades_file_pin_buckets_without_replacing_pin_or_mpu_data() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    upgrade_file_pin_bucket_acl(&db).await;
}

#[tokio::test]
#[ignore = "requires isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_upgrades_file_pin_buckets_without_replacing_pin_or_mpu_data() {
    postgres::with_schema(|db| async move { upgrade_file_pin_bucket_acl(&db).await }).await;
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
