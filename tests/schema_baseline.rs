use ipfs_s3_gateway::store::{
    self,
    initialization::{self, UpgradeRequest},
    migrations::{Historical, baseline, history},
    pinning::management::{native, schema},
};
use sea_orm::{ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement};
use sea_orm_migration::MigratorTrait;
#[path = "support/baseline_postgres.rs"]
mod postgres;

async fn exercise(db: &DatabaseConnection, source: Option<&str>) {
    if let Some(source) = source {
        Historical::up(db, Some(history::prefix(source).unwrap().len() as u32))
            .await
            .unwrap();
        db.execute_unprepared("INSERT INTO buckets(name,owner) VALUES('preserved','owner')")
            .await
            .unwrap();
        db.execute_unprepared("CREATE INDEX idx_custom_bucket_owner ON buckets(owner)")
            .await
            .unwrap();
        db.execute_unprepared(
            "CREATE INDEX idx_object_versions_object_id ON object_versions(object_id)",
        )
        .await
        .unwrap();
        db.execute_unprepared("CREATE INDEX idx_physical_residencies_unverified ON physical_residencies(tier,cid) WHERE verification_state='pending'").await.unwrap();
        if source != history::M23 {
            db.execute_unprepared("INSERT INTO import_jobs(id,bucket,key,source_type,source_value,request_fingerprint,metadata_json,tags_json,pin_decision_json,zip_intent_json,root_capture_json,object_content_type,decompress_prefix,state,phase,attempts,next_attempt_at,claim_epoch,providers_observed,pin_nodes_processed,pin_bytes_processed,downloaded_bytes,ipfs_add_bytes,entries_processed,entries_succeeded,entries_failed,decompressed_bytes,created_at,updated_at) VALUES ('old-job','preserved','key','cid','opaque original','fingerprint',' {\"duplicate\":0,\"duplicate\":1} ',' [] ',NULL,'null','','application/test','prefix','queued','queued',7,CURRENT_TIMESTAMP,19,0,0,0,0,0,0,0,0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
            db.execute_unprepared(
                "CREATE INDEX idx_custom_import_attempts ON import_jobs(attempts)",
            )
            .await
            .unwrap();
        }
        let binding = native::scope::binding(db).await.unwrap();
        assert!(initialization::open(db).await.is_err());
        let archive_count = || {
            Statement::from_string(
                db.get_database_backend(),
                "SELECT (SELECT COUNT(*) FROM pin_management_archive)+(SELECT COUNT(*) FROM pin_management_inline_archive) AS n",
            )
        };
        let archives_before = if source == history::M23 {
            Some(
                db.query_one(archive_count())
                    .await
                    .unwrap()
                    .unwrap()
                    .try_get::<i64>("", "n")
                    .unwrap(),
            )
        } else {
            None
        };
        let request = || UpgradeRequest {
            database_scope: &binding.0,
            native_identity: &binding.1,
            source_version: source,
            target_version: baseline::VERSION,
        };
        initialization::upgrade_existing(db, request())
            .await
            .unwrap_or_else(|e| panic!("{source}: {e}"));
        initialization::upgrade_existing(db, request())
            .await
            .unwrap();
        if let Some(before) = archives_before {
            assert_eq!(
                db.query_one(archive_count())
                    .await
                    .unwrap()
                    .unwrap()
                    .try_get::<i64>("", "n")
                    .unwrap(),
                before + 1
            );
        }
        assert_eq!(native::scope::binding(db).await.unwrap(), binding);
        let indexes = if db.get_database_backend() == DatabaseBackend::Sqlite {
            "SELECT name FROM sqlite_schema WHERE type='index' AND name IN ('idx_object_versions_object_id','idx_physical_residencies_unverified')"
        } else {
            "SELECT indexname AS name FROM pg_indexes WHERE schemaname=current_schema() AND indexname IN ('idx_object_versions_object_id','idx_physical_residencies_unverified')"
        };
        assert_eq!(
            db.query_all(Statement::from_string(db.get_database_backend(), indexes))
                .await
                .unwrap()
                .len(),
            2
        );
        if source != history::M23 {
            let row=db.query_one(Statement::from_string(db.get_database_backend(),"SELECT r.*,j.attempts,j.claim_epoch FROM import_job_requests r JOIN import_jobs j ON j.id=r.job_id WHERE job_id='old-job'")).await.unwrap().unwrap();
            assert_eq!(
                row.try_get::<String>("", "metadata_json").unwrap(),
                " {\"duplicate\":0,\"duplicate\":1} "
            );
            assert_eq!(
                row.try_get::<Option<String>>("", "pin_decision_json")
                    .unwrap(),
                None
            );
            assert_eq!(
                row.try_get::<String>("", "zip_intent_json").unwrap(),
                "null"
            );
            assert_eq!(row.try_get::<String>("", "root_capture_json").unwrap(), "");
            assert_eq!(row.try_get::<i64>("", "claim_epoch").unwrap(), 19);
        }
        let wrong = if source == history::M13 {
            history::M17
        } else {
            history::M13
        };
        assert!(
            initialization::upgrade_existing(
                db,
                UpgradeRequest {
                    source_version: wrong,
                    ..request()
                }
            )
            .await
            .is_err()
        );
        assert!(
            db.query_one(Statement::from_string(
                db.get_database_backend(),
                "SELECT owner FROM buckets WHERE owner='owner'"
            ))
            .await
            .unwrap()
            .is_some()
        );
    } else {
        initialization::open(db).await.unwrap();
    }
    initialization::open(db).await.unwrap();
    assert_eq!(schema::validate(db).await.unwrap(), [baseline::VERSION]);
    store::bucket::create(db, "baseline", None).await.unwrap();
    store::object::upsert(
        db, "object", "baseline", "key", "cid", 3, None, "cid", None, false, None, None, false,
    )
    .await
    .unwrap();
    assert_eq!(
        store::object::get_by_id(db, "object").await.unwrap().cid,
        "cid"
    );
}

#[tokio::test]
async fn sqlite_fresh_and_bound_historical_sources() {
    for source in [
        None,
        Some(history::M13),
        Some(history::M17),
        Some(history::M23),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            directory
                .path()
                .join("source.sqlite")
                .to_string_lossy()
                .replace('\\', "/")
        );
        let db = store::connect_database(&url).await.unwrap();
        exercise(&db, source).await;
        db.close().await.unwrap();
        let db = store::connect_database(&url).await.unwrap();
        initialization::open(&db).await.unwrap();
        assert_eq!(
            store::object::get_by_id(&db, "object").await.unwrap().size,
            3
        );
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn fresh_install_creates_final_layout_without_intermediate_ddl() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    let sql = store::migrations::baseline_install_statements(&db)
        .await
        .into_iter()
        .map(|sql| sql.to_ascii_lowercase())
        .collect::<Vec<_>>();
    assert!(
        sql.iter()
            .any(|sql| sql.contains("create table import_job_requests"))
    );
    assert!(
        !sql.iter()
            .any(|sql| sql.contains("alter table ") || sql.contains("drop table "))
    );
    assert!(!sql.iter().any(|sql| {
        sql.starts_with("insert into pin_management_archive_identity(archive_id) select")
    }));
    assert!(
        !sql.iter()
            .any(|sql| sql.starts_with("update pin_management_readiness"))
    );
}

#[tokio::test]
async fn old_target_stamp_and_interrupted_backfill_remain_recoverable() {
    use native::meter::Budget;
    use sea_orm::TransactionTrait;
    use store::pinning::management::records::segmented::{ArchiveHeader, write_archive};
    for source in [history::M13, history::M17] {
        let directory = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            directory
                .path()
                .join("resume.sqlite")
                .to_string_lossy()
                .replace('\\', "/")
        );
        let db = store::connect_database(&url).await.unwrap();
        Historical::up(&db, None).await.unwrap();
        let (scope, identity) = native::scope::binding(&db).await.unwrap();
        let stamp=serde_json::to_vec(&serde_json::json!({"scope":scope,"identity":identity,"source":source,"target":history::M23})).unwrap();
        let id = format!("history:upgrade:{}", history::M23);
        let tx = db.begin().await.unwrap();
        write_archive(
            &tx,
            &id,
            &ArchiveHeader {
                database_scope: scope.clone(),
                database_identity: identity.clone(),
                principal: "explicit_local_upgrade".into(),
                catalog: vec![source.into(), history::M23.into()],
                request: String::new(),
                configuration: String::new(),
                acknowledgements: vec![],
                created_at: String::new(),
                expires_at: None,
            },
            &mut stamp.as_slice(),
            &mut Budget::default(),
        )
        .await
        .unwrap();
        tx.execute_unprepared("UPDATE pin_management_readiness SET state='backfill'")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let original = db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "SELECT header,payload FROM pin_management_inline_archive WHERE archive_id=$1",
                [id.clone().into()],
            ))
            .await
            .unwrap()
            .unwrap();
        let bytes: Vec<u8> = original.try_get("", "payload").unwrap();
        let header: String = original.try_get("", "header").unwrap();
        let request = || UpgradeRequest {
            database_scope: &scope,
            native_identity: &identity,
            source_version: source,
            target_version: baseline::VERSION,
        };
        assert!(initialization::open(&db).await.is_err());
        db.close().await.unwrap();
        let db = store::connect_database(&url).await.unwrap();
        initialization::upgrade_existing(&db, request())
            .await
            .unwrap();
        initialization::upgrade_existing(&db, request())
            .await
            .unwrap();
        let after = db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "SELECT header,payload FROM pin_management_inline_archive WHERE archive_id=$1",
                [id.into()],
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.try_get::<Vec<u8>>("", "payload").unwrap(), bytes);
        assert_eq!(after.try_get::<String>("", "header").unwrap(), header);
        initialization::open(&db).await.unwrap();
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn ledger_failure_rolls_back_m23_conversion_and_preserves_source() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("rollback.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    );
    let db = store::connect_database(&url).await.unwrap();
    Historical::up(&db, None).await.unwrap();
    db.execute_unprepared("INSERT INTO buckets(name,owner) VALUES('before','preserve')")
        .await
        .unwrap();
    let (scope, identity) = native::scope::binding(&db).await.unwrap();
    db.execute_unprepared("CREATE TRIGGER fail_normalization BEFORE DELETE ON seaql_migrations BEGIN SELECT RAISE(ABORT,'injected ledger failure'); END").await.unwrap();
    let request = || UpgradeRequest {
        database_scope: &scope,
        native_identity: &identity,
        source_version: history::M23,
        target_version: baseline::VERSION,
    };
    assert!(
        initialization::upgrade_existing(&db, request())
            .await
            .is_err()
    );
    let versions = db
        .query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT version FROM seaql_migrations ORDER BY version",
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.try_get::<String>("", "version").unwrap())
        .collect::<Vec<_>>();
    assert!(history::is_m23(&versions));
    assert!(db.query_one(Statement::from_string(DatabaseBackend::Sqlite,"SELECT 1 FROM pin_management_inline_archive WHERE archive_id='history:upgrade:m20261027_000001_schema_baseline'")).await.unwrap().is_none());
    db.execute_unprepared("DROP TRIGGER fail_normalization")
        .await
        .unwrap();
    initialization::upgrade_existing(&db, request())
        .await
        .unwrap();
    assert_eq!(
        db.query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT owner FROM buckets WHERE name='before'"
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<String>("", "owner")
        .unwrap(),
        "preserve"
    );
    db.close().await.unwrap();
}

async fn corrupt_source_is_rejected_without_preparation(
    db: &DatabaseConnection,
    source: &str,
    damage: &str,
) {
    Historical::up(db, Some(history::prefix(source).unwrap().len() as u32))
        .await
        .unwrap();
    if source == history::M17 {
        db.execute_unprepared("INSERT INTO import_jobs(id,bucket,key,source_type,source_value,request_fingerprint,metadata_json,tags_json,state,phase,attempts,next_attempt_at,claim_epoch,providers_observed,pin_nodes_processed,pin_bytes_processed,downloaded_bytes,ipfs_add_bytes,entries_processed,entries_succeeded,entries_failed,decompressed_bytes,created_at,updated_at) VALUES('retained','bucket','key','cid','original request','fingerprint',' {\"duplicate\":0,\"duplicate\":1} ',' [] ','queued','queued',7,CURRENT_TIMESTAMP,19,0,0,0,0,0,0,0,0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    }
    let (scope, identity) = native::scope::binding(db).await.unwrap();
    db.execute_unprepared(damage).await.unwrap();
    let versions = || {
        Statement::from_string(
            db.get_database_backend(),
            "SELECT version FROM seaql_migrations ORDER BY version",
        )
    };
    let before = db
        .query_all(versions())
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.try_get::<String>("", "version").unwrap())
        .collect::<Vec<_>>();
    assert!(
        initialization::upgrade_existing(
            db,
            UpgradeRequest {
                database_scope: &scope,
                native_identity: &identity,
                source_version: source,
                target_version: baseline::VERSION,
            }
        )
        .await
        .is_err()
    );
    assert_eq!(
        db.query_all(versions())
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.try_get::<String>("", "version").unwrap())
            .collect::<Vec<_>>(),
        before
    );
    let stamps = db.query_one(Statement::from_string(db.get_database_backend(), "SELECT COUNT(*) AS n FROM pin_management_archive WHERE archive_id LIKE 'history:upgrade:%'")).await.unwrap().unwrap().try_get::<i64>("", "n").unwrap();
    assert_eq!(stamps, 0);
    if source == history::M17 {
        // These remain the real document-era source columns after the failed preparation.
        db.query_all(Statement::from_string(db.get_database_backend(), "SELECT source_value,metadata_json,tags_json,pin_decision_json,zip_intent_json,root_capture_json,object_content_type,decompress_prefix FROM import_jobs")).await.unwrap();
        let row = db.query_one(Statement::from_string(db.get_database_backend(), "SELECT source_value,metadata_json,claim_epoch FROM import_jobs WHERE id='retained'")).await.unwrap().unwrap();
        assert_eq!(
            row.try_get::<String>("", "source_value").unwrap(),
            "original request"
        );
        assert_eq!(
            row.try_get::<String>("", "metadata_json").unwrap(),
            " {\"duplicate\":0,\"duplicate\":1} "
        );
        assert_eq!(row.try_get::<i64>("", "claim_epoch").unwrap(), 19);
        assert!(
            db.query_one(Statement::from_string(
                db.get_database_backend(),
                "SELECT 1 FROM import_job_requests"
            ))
            .await
            .is_err()
        );
    } else {
        let stamps = db.query_one(Statement::from_string(db.get_database_backend(), "SELECT COUNT(*) AS n FROM pin_management_inline_archive WHERE archive_id LIKE 'history:upgrade:%'")).await.unwrap().unwrap().try_get::<i64>("", "n").unwrap();
        assert_eq!(stamps, 0);
    }
}

#[tokio::test]
async fn corrupt_source_never_commits_preparation() {
    for (source, damage) in [
        (history::M23, "DROP TABLE bucket_cors_configs"),
        (history::M23, "DROP TABLE object_tags"),
        (history::M23, "DROP TABLE bucket_lifecycle_configs"),
        (history::M23, "DROP TABLE residency_backfill"),
        (history::M23, "DROP TABLE zip_mpu_replays"),
        (history::M17, "DROP TRIGGER s75_archive_no_delete"),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let db = store::connect_database(&format!(
            "sqlite://{}?mode=rwc",
            directory
                .path()
                .join("corrupt.sqlite")
                .to_string_lossy()
                .replace('\\', "/")
        ))
        .await
        .unwrap();
        corrupt_source_is_rejected_without_preparation(&db, source, damage).await;
        db.close().await.unwrap();
    }
    let db = Database::connect("sqlite::memory:").await.unwrap();
    initialization::open(&db).await.unwrap();
    let ddl = db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT sql FROM sqlite_schema WHERE name='bucket_cors_configs'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<String>("", "sql")
        .unwrap();
    db.execute_unprepared(&ddl.replace(
        "CREATE TABLE bucket_cors_configs",
        "CREATE TEMP TABLE BUCKET_CORS_CONFIGS",
    ))
    .await
    .unwrap();
    assert!(initialization::validate_existing(&db).await.is_err());
    db.execute_unprepared("DROP TABLE temp.BUCKET_CORS_CONFIGS")
        .await
        .unwrap();
    db.execute_unprepared("DROP TABLE bucket_cors_configs")
        .await
        .unwrap();
    assert!(initialization::validate_existing(&db).await.is_err());
    assert!(initialization::open(&db).await.is_err());
}

#[tokio::test]
#[ignore = "requires authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_fresh_and_bound_historical_sources() {
    use futures_util::FutureExt;
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL").unwrap();
    let admin = Database::connect(&url).await.unwrap();
    for source in [
        None,
        Some(history::M13),
        Some(history::M17),
        Some(history::M23),
    ] {
        let name = format!("baseline_{}", uuid::Uuid::new_v4().simple());
        admin
            .execute_unprepared(&format!("CREATE SCHEMA \"{name}\""))
            .await
            .unwrap();
        let mut options = sea_orm::ConnectOptions::new(&url);
        options
            .max_connections(4)
            .set_schema_search_path(name.clone());
        let db = Database::connect(options).await.unwrap();
        assert_eq!(db.get_database_backend(), DatabaseBackend::Postgres);
        let result = std::panic::AssertUnwindSafe(exercise(&db, source))
            .catch_unwind()
            .await;
        db.close().await.unwrap();
        admin
            .execute_unprepared(&format!("DROP SCHEMA \"{name}\" CASCADE"))
            .await
            .unwrap();
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    }
    admin.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_corrupt_sources_never_commit_preparation() {
    for (source, damage) in [
        (history::M23, "DROP TABLE bucket_cors_configs"),
        (history::M23, "DROP TABLE object_tags"),
        (history::M23, "DROP TABLE bucket_lifecycle_configs"),
        (history::M23, "DROP TABLE residency_backfill"),
        (history::M23, "DROP TABLE zip_mpu_replays"),
        (
            history::M17,
            "DROP TRIGGER s75_archive_no_delete ON pin_management_archive",
        ),
    ] {
        postgres::with_schema(|db| async move {
            corrupt_source_is_rejected_without_preparation(&db, source, damage).await;
        })
        .await;
    }
}

#[tokio::test]
#[ignore = "requires authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_old_target_stamp_recovers_without_rewriting_original() {
    use native::meter::Budget;
    use sea_orm::TransactionTrait;
    use store::pinning::management::records::segmented::{ArchiveHeader, write_archive};
    for source in [history::M13, history::M17] {
        postgres::with_schema(|db| async move {
            Historical::up(&db, None).await.unwrap();
            let (scope, identity) = native::scope::binding(&db).await.unwrap();
            let id = format!("history:upgrade:{}", history::M23);
            let original = serde_json::to_vec(&serde_json::json!({"scope":scope,"identity":identity,"source":source,"target":history::M23})).unwrap();
            let tx = db.begin().await.unwrap();
            let header = ArchiveHeader { database_scope:scope.clone(), database_identity:identity.clone(), principal:"explicit_local_upgrade".into(), catalog:vec![source.into(), history::M23.into()], request:String::new(), configuration:String::new(), acknowledgements:vec![], created_at:String::new(), expires_at:None };
            write_archive(&tx, &id, &header, &mut original.as_slice(), &mut Budget::default()).await.unwrap();
            tx.execute_unprepared("UPDATE pin_management_readiness SET state='backfill'").await.unwrap();
            tx.commit().await.unwrap();
            initialization::upgrade_existing(&db, UpgradeRequest { database_scope:&scope, native_identity:&identity, source_version:source, target_version:baseline::VERSION }).await.unwrap();
            let row = db.query_one(Statement::from_sql_and_values(DatabaseBackend::Postgres, "SELECT payload,header FROM pin_management_inline_archive WHERE archive_id=$1", [id.into()])).await.unwrap().unwrap();
            assert_eq!(row.try_get::<Vec<u8>>("", "payload").unwrap(), original);
            assert_eq!(row.try_get::<String>("", "header").unwrap(), serde_json::to_string(&header).unwrap());
            initialization::open(&db).await.unwrap();
        }).await;
    }
}
