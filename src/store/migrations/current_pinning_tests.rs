use crate::store::{
    initialization,
    migrator::Migrator,
    pinning::management::{native, schema},
};
use sea_orm::{ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement};
use sea_orm_migration::MigratorTrait;

async fn baseline() -> (tempfile::TempDir, DatabaseConnection) {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("baseline.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    );
    let db = crate::store::connect_database(&url).await.unwrap();
    Migrator::up(&db, Some(1)).await.unwrap();
    db.execute_unprepared("INSERT INTO buckets(name) VALUES ('bucket'); INSERT INTO objects(id,bucket,key,cid,size,etag) VALUES ('object','bucket','key','cid',1,'cid'); INSERT INTO object_versions(id,bucket,key,kind,object_id,sequence,is_latest,lifecycle_age_started_at,created_at,updated_at) VALUES ('version','bucket','key','object','object',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO pin_extension_decisions VALUES ('version','object','control','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','accepted','{\"original\":true}'); INSERT INTO pin_leases VALUES ('lease','object','source','policy','mode','mode',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,1,'active'); INSERT INTO pin_lease_targets VALUES ('target','lease','cid',1,'provider','waiting',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); INSERT INTO pin_jobs(id,operation,provider,cid,expected_remote_epoch,submit_phase,state,next_attempt_at,last_error) VALUES ('job','unpin','provider','cid',1,NULL,'done',CURRENT_TIMESTAMP,'original error')").await.unwrap();
    db.execute_unprepared("INSERT INTO multipart_uploads(upload_id,object_id,bucket,key) VALUES ('upload','future','bucket','archive'); INSERT INTO zip_v2_executions(id,owner,source,token,request_fingerprint,request_contract,bucket,source_key,captured_options,created_at,updated_at) VALUES ('execution','owner','mpu','token','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',' original request ','bucket','archive','{\"rule_revision\":\"historical\"}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); INSERT INTO zip_v2_mpu_intakes VALUES ('upload','upload','execution','owner','token','bucket','archive','out/','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','{\"rule_revision\":\"historical\"}',' original config ','historical',CURRENT_TIMESTAMP); INSERT INTO zip_v2_mpu_completions VALUES ('upload','owner','token','execution','bucket','archive',' original parts ','<original/>',' original headers ')").await.unwrap();
    (directory, db)
}

async fn upgrade(db: &DatabaseConnection) -> Result<(), sea_orm::DbErr> {
    let (scope, identity) = native::scope::binding(db).await.unwrap();
    initialization::upgrade_existing(
        db,
        initialization::UpgradeRequest {
            native_identity: &identity,
            database_scope: &scope,
            source_version: super::baseline::VERSION,
            target_version: schema::VERSION,
        },
    )
    .await
}

async fn catalog(db: &DatabaseConnection) -> Vec<(String, String, String)> {
    db.query_all(Statement::from_string(DatabaseBackend::Sqlite,
        "SELECT type,name,sql FROM sqlite_schema WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' AND name<>'_ipfs_s3_migration_lock' ORDER BY type,name"))
        .await.unwrap().iter().map(|r| (r.try_get("", "type").unwrap(), r.try_get("", "name").unwrap(), r.try_get("", "sql").unwrap())).collect()
}

#[tokio::test]
async fn old_baseline_requires_explicit_upgrade_and_preserves_data() {
    let (_directory, db) = baseline().await;
    let old_catalog = catalog(&db).await;
    assert!(initialization::open(&db).await.is_err());
    assert_eq!(old_catalog, catalog(&db).await);
    upgrade(&db).await.unwrap();
    initialization::open(&db).await.unwrap();
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT state,last_error FROM pin_jobs WHERE id='job'",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<String>("", "state").unwrap(), "done");
    assert_eq!(
        row.try_get::<String>("", "last_error").unwrap(),
        "original error"
    );
    let row = db.query_one(Statement::from_string(DatabaseBackend::Sqlite,
        "SELECT snapshot,control_revision FROM pin_extension_decisions WHERE version_row_id='version'")).await.unwrap().unwrap();
    assert_eq!(
        row.try_get::<String>("", "snapshot").unwrap(),
        "{\"original\":true}"
    );
    assert_eq!(
        row.try_get::<String>("", "control_revision").unwrap(),
        "control"
    );
    let intake = crate::store::multipart::v2_zip::read_by_upload(&db, "upload")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        intake.captured_options,
        "{\"rule_revision\":\"historical\"}"
    );
    assert_eq!(intake.captured_config, " original config ");
    assert_eq!(intake.request_fingerprint, "a".repeat(64));
    let receipt = db.query_one(Statement::from_string(DatabaseBackend::Sqlite,
        "SELECT part_contract,response_xml,response_headers FROM zip_v2_mpu_completions WHERE upload_id='upload'")).await.unwrap().unwrap();
    assert_eq!(
        receipt.try_get::<String>("", "part_contract").unwrap(),
        " original parts "
    );
    assert_eq!(
        receipt.try_get::<String>("", "response_xml").unwrap(),
        "<original/>"
    );
    assert_eq!(
        receipt.try_get::<String>("", "response_headers").unwrap(),
        " original headers "
    );
    assert!(
        db.execute_unprepared("DELETE FROM zip_v2_mpu_intakes WHERE original_upload_id='upload'")
            .await
            .is_err()
    );
    db.execute_unprepared("DELETE FROM multipart_uploads WHERE upload_id='upload'")
        .await
        .unwrap();
    assert!(
        crate::store::multipart::v2_zip::read_by_upload(&db, "upload")
            .await
            .unwrap()
            .unwrap()
            .active_upload_id
            .is_none()
    );
    assert!(db.execute_unprepared("INSERT INTO pin_lease_targets(id,lease_id,cid,logical_size,provider,state,created_at,last_touched_at) SELECT 'duplicate',lease_id,cid,logical_size,provider,state,created_at,last_touched_at FROM pin_lease_targets WHERE id='target'").await.is_err());
    db.execute_unprepared("UPDATE pin_jobs SET state='error'; UPDATE pin_lease_targets SET state='error',last_error='policy unavailable'").await.unwrap();
    assert!(
        db.execute_unprepared("UPDATE pin_jobs SET state='other'")
            .await
            .is_err()
    );
    assert!(
        db.execute_unprepared("UPDATE pin_lease_targets SET state='other'")
            .await
            .is_err()
    );
    assert!(
        db.execute_unprepared("UPDATE pin_lease_targets SET lease_id='missing'")
            .await
            .is_err()
    );
    initialization::open(&db).await.unwrap();
    let empty = Database::connect("sqlite::memory:").await.unwrap();
    initialization::initialize_empty(&empty).await.unwrap();
    assert_eq!(catalog(&db).await, catalog(&empty).await);
    assert!(
        db.query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA foreign_key_check"
        ))
        .await
        .unwrap()
        .is_empty()
    );
}

#[tokio::test]
async fn failed_rebuild_rolls_back_original_schema_rows_and_ledger() {
    let (_directory, db) = baseline().await;
    db.execute_unprepared("PRAGMA ignore_check_constraints=ON; UPDATE pin_lease_targets SET state='invalid'; PRAGMA ignore_check_constraints=OFF").await.unwrap();
    let before = catalog(&db).await;
    assert!(upgrade(&db).await.is_err());
    assert_eq!(before, catalog(&db).await);
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT state FROM pin_lease_targets WHERE id='target'",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<String>("", "state").unwrap(), "invalid");
    let versions = db
        .query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT version FROM seaql_migrations ORDER BY version",
        ))
        .await
        .unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(
        versions[0].try_get::<String>("", "version").unwrap(),
        super::baseline::VERSION
    );
}

#[tokio::test]
async fn external_cascade_dependents_refuse_upgrade_without_losing_rows() {
    let (_directory, db) = baseline().await;
    db.execute_unprepared("CREATE TABLE extra_job_notes(job TEXT PRIMARY KEY REFERENCES pin_jobs(id) ON DELETE CASCADE, note TEXT NOT NULL); INSERT INTO extra_job_notes VALUES ('job','preserve external data')").await.unwrap();
    let before = catalog(&db).await;
    assert!(upgrade(&db).await.is_err());
    assert_eq!(catalog(&db).await, before);
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT note FROM extra_job_notes WHERE job='job'",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.try_get::<String>("", "note").unwrap(),
        "preserve external data"
    );
}

#[tokio::test]
async fn additional_inline_constraints_refuse_rebuild_without_weakening_data_guards() {
    for constraint in ["CHECK(state<>'running')", "UNIQUE"] {
        let (_directory, db) = baseline().await;
        let definition: String = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT sql FROM sqlite_schema WHERE type='table' AND name='pin_jobs'",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "sql")
            .unwrap();
        let retained = db.query_all(Statement::from_string(DatabaseBackend::Sqlite,
            "SELECT sql FROM sqlite_schema WHERE tbl_name='pin_jobs' AND type IN ('index','trigger') AND sql IS NOT NULL")).await.unwrap();
        db.execute_unprepared(
            "CREATE TEMP TABLE original_jobs AS SELECT * FROM pin_jobs; DROP TABLE pin_jobs",
        )
        .await
        .unwrap();
        db.execute_unprepared(&definition.replace(
            "state TEXT NOT NULL DEFAULT 'pending'",
            &format!("state TEXT NOT NULL DEFAULT 'pending' {constraint}"),
        ))
        .await
        .unwrap();
        db.execute_unprepared(
            "INSERT INTO pin_jobs SELECT * FROM original_jobs; DROP TABLE original_jobs",
        )
        .await
        .unwrap();
        for row in retained {
            db.execute_unprepared(&row.try_get::<String>("", "sql").unwrap())
                .await
                .unwrap();
        }
        crate::store::pinning::management::catalog::validate(
            &db,
            native::archived_catalog(&[super::baseline::VERSION.into()]).unwrap(),
        )
        .await
        .unwrap();
        let before = catalog(&db).await;
        assert!(upgrade(&db).await.is_err());
        assert_eq!(catalog(&db).await, before);
        let row = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT state FROM pin_jobs WHERE id='job'",
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.try_get::<String>("", "state").unwrap(), "done");
        let rejected = if constraint == "UNIQUE" {
            "INSERT INTO pin_jobs(id,operation,provider,cid,expected_remote_epoch,submit_phase,state,next_attempt_at) VALUES ('duplicate','unpin','provider','cid',1,NULL,'done',CURRENT_TIMESTAMP)"
        } else {
            "UPDATE pin_jobs SET state='running' WHERE id='job'"
        };
        assert!(db.execute_unprepared(rejected).await.is_err());
    }
}

#[test]
fn historical_catalog_is_not_the_current_catalog() {
    let old = native::archived_catalog(&[super::baseline::VERSION.into()]).unwrap();
    let current = native::current_catalog();
    let columns = |tables: &[native::NativeTable], name: &str| {
        tables
            .iter()
            .find(|t| t.table == name)
            .unwrap()
            .columns
            .to_vec()
    };
    assert!(columns(old, "pin_extension_decisions").contains(&"config_revision".into()));
    assert!(!columns(current, "pin_extension_decisions").contains(&"config_revision".into()));
    assert!(columns(old, "zip_v2_mpu_intakes").contains(&"rule_revision".into()));
    assert!(!columns(current, "zip_v2_mpu_intakes").contains(&"rule_revision".into()));
    assert!(!columns(old, "pin_lease_targets").contains(&"last_error".into()));
    assert!(columns(current, "pin_lease_targets").contains(&"last_error".into()));
}
