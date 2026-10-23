use super::*;
use crate::store::{
    self,
    pinning::management::{native, schema},
};

#[tokio::test]
async fn old_layout_upgrade_preserves_frozen_import_bytes_and_hot_state() {
    let directory = tempfile::tempdir().unwrap();
    let mut options = sea_orm::ConnectOptions::new(format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("source.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    ));
    // total_changes() is connection-local; this fixture exercises one owner.
    options.max_connections(1).min_connections(1);
    let db = sea_orm::Database::connect(options).await.unwrap();
    let old = "m20261017_000001_management_storage";
    let count = store::migrator::Migrator::migrations()
        .iter()
        .position(|migration| migration.name() == old)
        .unwrap() as u32
        + 1;
    store::migrator::Migrator::up(&db, Some(count))
        .await
        .unwrap();
    let (scope, identity) = native::scope::binding(&db).await.unwrap();
    let raw = format!(
        " {{\"duplicate\":null,\"duplicate\":\"{}\"}} \n",
        "界".repeat(400_000)
    );
    let created = "2026-10-01T01:02:03.123456789Z";
    let job_id = format!("old-{}", "界".repeat(1000));
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO import_jobs(metadata_json,created_at,id,bucket,key,source_type,source_value,request_fingerprint,tags_json,pin_decision_json,zip_intent_json,root_capture_json,state,phase,attempts,next_attempt_at,claim_epoch,providers_observed,pin_nodes_processed,pin_bytes_processed,downloaded_bytes,ipfs_add_bytes,entries_processed,entries_succeeded,entries_failed,decompressed_bytes,updated_at) VALUES ($1,$2,$3,'bucket','key','cid','opaque original source','fingerprint',' [] ',NULL,'null','', 'queued','queued',7,$2,19,0,0,0,0,0,0,0,0,0,$2)",
        [raw.clone().into(), created.into(), job_id.clone().into()])).await.unwrap();
    let changes =
        || Statement::from_string(db.get_database_backend(), "SELECT total_changes() AS n");
    let before: i64 = db
        .query_one(changes())
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    assert!(store::run_migrations(&db).await.is_err());
    let after: i64 = db
        .query_one(changes())
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    assert_eq!(before, after);
    db.execute_unprepared("CREATE TRIGGER fail_request_revision BEFORE INSERT ON pin_management_revision WHEN NEW.source_table='import_job_requests' BEGIN SELECT RAISE(ABORT,'fixture conversion failure'); END").await.unwrap();
    let failure = upgrade_existing(
        &db,
        UpgradeRequest {
            native_identity: &identity,
            database_scope: &scope,
            source_version: old,
            target_version: schema::VERSION,
        },
    )
    .await
    .expect_err("the injected SQL failure must roll back conversion");
    assert!(matches!(failure, DbErr::Exec(_)), "{failure}");
    assert_eq!(classify(&db).await.unwrap(), SchemaState::UpgradeRequired);
    let original = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT metadata_json FROM import_jobs",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        original.try_get::<String>("", "metadata_json").unwrap(),
        raw
    );
    db.execute_unprepared("DROP TRIGGER fail_request_revision")
        .await
        .unwrap();
    for attempt in 1..=2 {
        upgrade_existing(
            &db,
            UpgradeRequest {
                native_identity: &identity,
                database_scope: &scope,
                source_version: old,
                target_version: schema::VERSION,
            },
        )
        .await
        .unwrap_or_else(|error| panic!("explicit upgrade attempt {attempt}: {error}"));
        validate_existing(&db).await.unwrap();
    }
    assert_eq!(classify(&db).await.unwrap(), SchemaState::Current);
    let row = db.query_one(Statement::from_string(db.get_database_backend(),
        "SELECT r.*,CAST(j.created_at AS TEXT) AS original_created,CAST(j.next_attempt_at AS TEXT) AS original_due,j.attempts,j.claim_epoch FROM import_jobs j JOIN import_job_requests r ON r.job_id=j.id"))
        .await.unwrap().unwrap();
    assert_eq!(row.try_get::<String>("", "job_id").unwrap(), job_id);
    assert_eq!(row.try_get::<String>("", "metadata_json").unwrap(), raw);
    assert_eq!(
        row.try_get::<Option<String>>("", "pin_decision_json")
            .unwrap(),
        None
    );
    assert_eq!(
        row.try_get::<Option<String>>("", "zip_intent_json")
            .unwrap(),
        Some("null".into())
    );
    assert_eq!(
        row.try_get::<Option<String>>("", "root_capture_json")
            .unwrap(),
        Some(String::new())
    );
    assert_eq!(
        row.try_get::<String>("", "original_created").unwrap(),
        created
    );
    assert_eq!(row.try_get::<String>("", "original_due").unwrap(), created);
    assert_eq!(row.try_get::<i32>("", "attempts").unwrap(), 7);
    assert_eq!(row.try_get::<i64>("", "claim_epoch").unwrap(), 19);
    // Hot progress writes never rewrite or normalize the frozen request.
    db.execute_unprepared("CREATE TRIGGER preserve_request BEFORE UPDATE ON import_job_requests BEGIN SELECT RAISE(ABORT,'frozen request'); END").await.unwrap();
    db.execute_unprepared("UPDATE import_jobs SET downloaded_bytes=10,phase='inspecting'")
        .await
        .unwrap();
    let frozen = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT metadata_json FROM import_job_requests",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frozen.try_get::<String>("", "metadata_json").unwrap(), raw);
    db.close().await.unwrap();
}
