use ipfs_s3_gateway::store::{
    initialization::{self, UpgradeRequest},
    migrations::{baseline, m20261027_000001_schema_baseline},
    pinning::management::{native, schema},
};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, DbErr, Statement};
use sea_orm_migration::{MigrationTrait, MigratorTrait};

struct Baseline;
#[async_trait::async_trait]
impl MigratorTrait for Baseline {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(m20261027_000001_schema_baseline::Migration)]
    }
}

pub async fn baseline(db: &DatabaseConnection) {
    Baseline::up(db, None).await.unwrap();
    db.execute_unprepared("INSERT INTO buckets(name) VALUES ('bucket'); INSERT INTO objects(id,bucket,key,cid,size,etag) VALUES ('object','bucket','key','cid',1,'cid'); INSERT INTO object_versions(id,bucket,key,kind,object_id,sequence,is_latest,lifecycle_age_started_at,created_at,updated_at) VALUES ('version','bucket','key','object','object',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO pin_extension_decisions VALUES ('version','object','control','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','accepted',' {\"original\": true} '); INSERT INTO pin_leases VALUES ('lease','object','source','policy','mode','mode',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,1,'active'); INSERT INTO pin_lease_targets VALUES ('target','lease','cid',1,'provider','waiting',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); INSERT INTO pin_jobs(id,operation,provider,cid,expected_remote_epoch,submit_phase,state,next_attempt_at,last_error) VALUES ('job','unpin','provider','cid',1,NULL,'done',CURRENT_TIMESTAMP,' original error ')").await.unwrap();
    db.execute_unprepared("INSERT INTO multipart_uploads(upload_id,object_id,bucket,key) VALUES ('upload','future','bucket','archive'); INSERT INTO zip_v2_executions(id,owner,source,token,request_fingerprint,request_contract,bucket,source_key,captured_options,created_at,updated_at) VALUES ('execution','owner','mpu','token','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',' original request ','bucket','archive',' {\"rule_revision\": \"historical\"} ',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); INSERT INTO zip_v2_mpu_intakes VALUES ('upload','upload','execution','owner','token','bucket','archive','out/','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',' {\"rule_revision\": \"historical\"} ',' original config ','historical',CURRENT_TIMESTAMP); INSERT INTO zip_v2_mpu_completions VALUES ('upload','owner','token','execution','bucket','archive',' original parts ',' <original/> ',' original headers ')").await.unwrap();
}

pub async fn upgrade(db: &DatabaseConnection) -> Result<(), DbErr> {
    let (scope, identity) = native::scope::binding(db).await.unwrap();
    initialization::upgrade_existing(
        db,
        UpgradeRequest {
            database_scope: &scope,
            native_identity: &identity,
            source_version: baseline::VERSION,
            target_version: schema::VERSION,
        },
    )
    .await
}

pub async fn strings(db: &impl ConnectionTrait, query: &str) -> Vec<String> {
    db.query_all(Statement::from_string(DatabaseBackend::Postgres, query))
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.try_get("", "value").unwrap())
        .collect()
}

pub async fn snapshot(db: &impl ConnectionTrait) -> Vec<String> {
    let mut result = strings(db, "SELECT c.relname || ':' || a.attnum || ':' || a.attname || ':' || format_type(a.atttypid,a.atttypmod) || ':' || a.attnotnull AS value FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relkind='r' AND a.attnum>0 AND NOT a.attisdropped ORDER BY c.relname,a.attnum").await;
    result.extend(strings(db, "SELECT c.relname || ':' || k.conname || ':' || pg_get_constraintdef(k.oid,false) AS value FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() ORDER BY c.relname,k.conname").await);
    result.extend(strings(db, "SELECT indexname || ':' || indexdef AS value FROM pg_indexes WHERE schemaname=current_schema() ORDER BY indexname").await);
    for table in [
        "seaql_migrations",
        "pin_management_readiness",
        "pin_management_inline_archive",
        "pin_extension_decisions",
        "pin_leases",
        "pin_lease_targets",
        "pin_jobs",
        "zip_v2_executions",
        "zip_v2_mpu_intakes",
        "zip_v2_mpu_completions",
        "multipart_uploads",
    ] {
        result.extend(strings(db, &format!("SELECT row_to_json(t)::text AS value FROM {table} t ORDER BY row_to_json(t)::text")).await);
    }
    result
}

pub async fn assert_history(db: &DatabaseConnection) {
    assert_eq!(
        strings(
            db,
            "SELECT state || ':' || last_error AS value FROM pin_jobs WHERE id='job'"
        )
        .await,
        ["done: original error "]
    );
    assert_eq!(strings(db, "SELECT control_revision || ':' || snapshot AS value FROM pin_extension_decisions WHERE version_row_id='version'").await, ["control: {\"original\": true} "]);
    let intake = ipfs_s3_gateway::store::multipart::v2_zip::read_by_upload(db, "upload")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        intake.captured_options,
        " {\"rule_revision\": \"historical\"} "
    );
    assert_eq!(intake.captured_config, " original config ");
    assert_eq!(intake.request_fingerprint, "a".repeat(64));
    assert_eq!(
        strings(
            db,
            "SELECT request_contract AS value FROM zip_v2_executions WHERE id='execution'"
        )
        .await,
        [" original request "]
    );
    assert_eq!(strings(db, "SELECT part_contract || ':' || response_xml || ':' || response_headers AS value FROM zip_v2_mpu_completions WHERE upload_id='upload'").await, [" original parts : <original/> : original headers "]);
}

pub async fn assert_current_guards(db: &DatabaseConnection) {
    db.execute_unprepared("UPDATE pin_jobs SET state='error'; UPDATE pin_lease_targets SET state='error',last_error='policy unavailable'").await.unwrap();
    for (rejected, code) in [
        ("UPDATE pin_jobs SET state='other'", "23514"),
        ("UPDATE pin_lease_targets SET state='other'", "23514"),
        ("UPDATE pin_lease_targets SET lease_id='missing'", "23503"),
        (
            "INSERT INTO pin_lease_targets(id,lease_id,cid,logical_size,provider,state,created_at,last_touched_at) SELECT 'duplicate',lease_id,cid,logical_size,provider,state,created_at,last_touched_at FROM pin_lease_targets WHERE id='target'",
            "23505",
        ),
        (
            "DELETE FROM zip_v2_mpu_intakes WHERE original_upload_id='upload'",
            "23001",
        ),
    ] {
        super::assert_sqlstate(db.execute_unprepared(rejected).await.unwrap_err(), code);
    }
    db.execute_unprepared("DELETE FROM multipart_uploads WHERE upload_id='upload'")
        .await
        .unwrap();
    assert!(
        ipfs_s3_gateway::store::multipart::v2_zip::read_by_upload(db, "upload")
            .await
            .unwrap()
            .unwrap()
            .active_upload_id
            .is_none()
    );
    initialization::open(db).await.unwrap();
}
