//! Upgrade actual Stage 2 data: reserved raw tags are not decision evidence.
use ipfs_s3_gateway::store::{self, migrations as m};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, Statement};
use sea_orm_migration::{MigrationTrait, MigratorTrait};

pub(crate) struct Stage2;

impl MigratorTrait for Stage2 {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        // The registered predecessor migrations in order, not synthetic DDL.
        vec![
            Box::new(m::m20250701_000001_init::Migration),
            Box::new(m::m20260707_000001_decompress_zip::Migration),
            Box::new(m::m20260720_000001_sse_c_key_fingerprint::Migration),
            Box::new(m::m20260721_000001_multi_provider_pinning::Migration),
            Box::new(m::m20260729_000001_ipfs3_import::Migration),
            Box::new(m::m20260729_000002_postgres_utc_timestamps::Migration),
            Box::new(m::m20260730_000001_standard_mutation_fence::Migration),
            Box::new(m::m20260813_000001_postgres_json_columns::Migration),
            Box::new(m::m20260825_000001_object_versioning::Migration),
            Box::new(m::m20260826_000001_lifecycle_expiration::Migration),
            Box::new(m::m20260831_000001_bucket_cors::Migration),
            Box::new(m::m20260901_000001_lifecycle_abort_multipart::Migration),
            Box::new(m::m20260912_000001_residency_references::Migration),
            Box::new(m::m20260912_000002_lifecycle_transition::Migration),
            Box::new(m::m20260919_000001_standard_mutation_lease::Migration),
            Box::new(m::m20260920_000001_pin_submit_history::Migration),
            Box::new(m::m20260920_000002_pin_identity_ledger::Migration),
        ]
    }
}

async fn count(db: &DatabaseConnection, table: &str) -> i64 {
    assert!(matches!(table, "pin_extension_decisions" | "object_tags"));
    db.query_one(Statement::from_string(
        db.get_database_backend(),
        format!("SELECT COUNT(*) AS n FROM {table}"),
    ))
    .await
    .unwrap()
    .unwrap()
    .try_get("", "n")
    .unwrap()
}

pub(crate) async fn upgrade_with_historical_raw_tags(db: &DatabaseConnection) {
    assert_eq!(Stage2::migrations().len(), 17);
    Stage2::up(db, None).await.unwrap();
    store::bucket::create(db, "stage3-history", None)
        .await
        .unwrap();
    store::object::upsert(
        db,
        "historical-object",
        "stage3-history",
        "old-key",
        "bafy-old",
        1,
        None,
        "bafy-old",
        None,
        false,
        None,
        None,
        false,
    )
    .await
    .unwrap();
    db.execute_unprepared("INSERT INTO object_versions (id,bucket,key,kind,object_id,sequence,is_latest,lifecycle_age_started_at,created_at,updated_at) VALUES ('historical-version','stage3-history','old-key','object','historical-object',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO object_tags (object_id, key, value) VALUES ('historical-object', 'ipfs-s3:pin', 'true')").await.unwrap();
    db.execute_unprepared("INSERT INTO multipart_uploads (upload_id,object_id,bucket,key,tags_json) VALUES ('historical-upload','future-object','stage3-history','old-upload','[{\"key\":\"ipfs-s3:pin\",\"value\":\"true\"}]')").await.unwrap();
    db.execute_unprepared(
        "INSERT INTO import_jobs (id,bucket,key,source_type,source_value,request_fingerprint,metadata_json,tags_json,state,phase,attempts,next_attempt_at,claim_epoch,providers_observed,pin_nodes_processed,pin_bytes_processed,downloaded_bytes,ipfs_add_bytes,entries_processed,entries_succeeded,entries_failed,decompressed_bytes,created_at,updated_at) VALUES ('historical-job','stage3-history','old-import','cid','bafy-old','old-request','{}','[{\"key\":\"ipfs-s3:pin\",\"value\":\"true\"}]','queued','queued',0,CURRENT_TIMESTAMP,0,0,0,0,0,0,0,0,0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ).await.unwrap();

    store::run_migrations(db).await.unwrap();
    // The idempotent startup path must not backfill decisions on a second run.
    store::run_migrations(db).await.unwrap();
    assert_eq!(count(db, "pin_extension_decisions").await, 0);
    assert_eq!(count(db, "object_tags").await, 1);
    let version = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT id FROM object_versions WHERE object_id='historical-object'",
        ))
        .await
        .unwrap()
        .unwrap();
    let version_id: String = version.try_get("", "id").unwrap();
    assert!(
        store::pinning::decision::read_for_version(db, &version_id)
            .await
            .unwrap()
            .is_none()
    );
    let upload = store::multipart::get_upload(db, "historical-upload")
        .await
        .unwrap();
    assert_eq!(upload.tags_json[0]["key"], "ipfs-s3:pin");
    assert!(
        store::multipart::decision_from_upload(&upload)
            .unwrap()
            .is_none()
    );
    let job = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT tags_json, pin_decision_json FROM import_jobs WHERE id='historical-job'",
        ))
        .await
        .unwrap()
        .unwrap();
    let tags: String = job.try_get("", "tags_json").unwrap();
    let decision: Option<String> = job.try_get("", "pin_decision_json").unwrap();
    assert!(tags.contains("ipfs-s3:pin"));
    assert!(decision.is_none());
}

#[tokio::test]
async fn sqlite_stage2_upgrade_does_not_promote_historical_control_tags() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .unwrap();
    upgrade_with_historical_raw_tags(&db).await;
}
