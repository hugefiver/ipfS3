//! Real PG17 race target: run explicitly with IPFS_S3_TEST_POSTGRES_URL and --ignored.
//! Only this test's UUID-named schema is created/dropped; never touches public.
use std::{future::Future, panic::AssertUnwindSafe, time::Duration};

use futures_util::FutureExt;
use ipfs_s3_gateway::error::AppError;
use ipfs_s3_gateway::store::{
    self,
    zip::{self, BatchAdmission, ManifestItem, RootOutcome, VersionBinding},
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, DbErr,
    Statement, TransactionTrait,
};
use sea_orm_migration::{MigrationTrait, MigratorTrait, SchemaManager};

async fn isolated<F, Fut>(migrate: bool, body: F)
where
    F: FnOnce(DatabaseConnection, DatabaseConnection) -> Fut,
    Fut: Future<Output = ()>,
{
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL")
        .expect("NOT RUN: set IPFS_S3_TEST_POSTGRES_URL to an authorized isolated PG17 database");
    let admin = Database::connect(&url).await.unwrap();
    let version: String = admin
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SHOW server_version",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "server_version")
        .unwrap();
    assert!(version.starts_with("17."), "requires PostgreSQL 17");
    let schema = format!("zip_batch_{}", uuid::Uuid::new_v4().simple());
    assert!(
        schema
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    );
    let schema_exists = admin
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT 1 FROM pg_namespace WHERE nspname=$1",
            [schema.clone().into()],
        ))
        .await
        .unwrap();
    assert!(schema_exists.is_none(), "test schema already existed");
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let result = AssertUnwindSafe(async {
        let mut connections = Vec::new();
        for _ in 0..2 {
            let mut opts = ConnectOptions::new(&url);
            opts.min_connections(1).max_connections(1);
            let db = Database::connect(opts).await.unwrap();
            db.execute_unprepared(&format!("SET search_path TO {schema}"))
                .await
                .unwrap();
            db.execute_unprepared("SET statement_timeout TO '8s'")
                .await
                .unwrap();
            connections.push(db);
        }
        if migrate {
            store::run_migrations(&connections[0]).await.unwrap();
        }
        body(connections[0].clone(), connections[1].clone()).await;
        for db in connections {
            db.close().await.unwrap();
        }
    })
    .catch_unwind()
    .await;
    admin
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    assert!(
        admin
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "SELECT 1 FROM pg_namespace WHERE nspname=$1",
                [schema.into()],
            ))
            .await
            .unwrap()
            .is_none(),
        "test schema survived cleanup"
    );
    admin.close().await.unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

fn assert_pg_constraint(error: DbErr, sqlstate: &str, constraint: &str) {
    let detail = format!("{error:?}");
    assert!(
        detail.contains(&format!("code: \"{sqlstate}\"")) && detail.contains(constraint),
        "expected PostgreSQL SQLSTATE {sqlstate} on {constraint}, got {detail}"
    );
}

async fn rejects_sql(db: &DatabaseConnection, sql: &str, sqlstate: &str, constraint: &str) {
    assert_pg_constraint(
        db.execute_unprepared(sql).await.unwrap_err(),
        sqlstate,
        constraint,
    );
}

fn assert_stale(error: AppError) {
    assert!(
        matches!(error, AppError::Internal(ref message)
        if message == "stale ZIP batch ownership or root claim"),
        "expected stale claim, got {error:?}"
    );
}

fn admission(id: &str) -> BatchAdmission {
    BatchAdmission {
        id: id.into(),
        owner: "owner".into(),
        source: "direct".into(),
        token: "token".into(),
        fingerprint: "fingerprint".into(),
        bucket: "bucket".into(),
        archive_key: "archive".into(),
        input_identity: "digest".into(),
        captured_options: "{}".into(),
    }
}

async fn prepared_output(db: &DatabaseConnection) {
    store::bucket::create(db, "bucket", None).await.unwrap();
    zip::admit(db, &admission("batch")).await.unwrap();
    zip::prepare_manifest(
        db,
        "batch",
        &[ManifestItem::Success {
            path: "a".into(),
            object_key: "out/a".into(),
            cid: "leaf".into(),
            size: 1,
        }],
    )
    .await
    .unwrap();
    db.execute_unprepared("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('obj','bucket','out/a','leaf',1,'leaf')").await.unwrap();
    db.execute_unprepared("INSERT INTO object_versions (id,bucket,key,kind,object_id,sequence,is_latest,created_at,updated_at,lifecycle_age_started_at) VALUES ('v1','bucket','out/a','object','obj',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
}

#[tokio::test]
#[ignore = "NOT RUN by default: requires IPFS_S3_TEST_POSTGRES_URL to PG17"]
async fn pg17_schema_rejects_invalid_batch_manifest_and_root_evidence() {
    isolated(true, |db, _| async move {
        prepared_output(&db).await;
        rejects_sql(&db, "UPDATE zip_batches SET state='published' WHERE id='batch'", "23514", "ck_zip_batch_terminal").await;
        rejects_sql(&db, "UPDATE zip_batches SET root_status='complete' WHERE id='batch'", "23514", "ck_zip_batch_root_shape").await;
        rejects_sql(&db, "UPDATE zip_batches SET root_revision=-1 WHERE id='batch'", "23514", "ck_zip_batch_revision").await;
        rejects_sql(&db, "UPDATE zip_batches SET source='wrong' WHERE id='batch'", "23514", "ck_zip_batch_source").await;
        rejects_sql(&db, "UPDATE zip_manifest_entries SET cid=NULL WHERE batch_id='batch' AND path='a'", "23514", "ck_zip_manifest_shape").await;
        rejects_sql(&db, "INSERT INTO zip_manifest_entries (batch_id,path,error_code,created_at) VALUES ('missing','bad','error',CURRENT_TIMESTAMP)", "23503", "zip_manifest_entries_batch_id_fkey").await;
        rejects_sql(&db, "INSERT INTO zip_root_builds (batch_id,revision,epoch,worker,lease_until,status,created_at,updated_at) VALUES ('batch',0,1,'worker',CURRENT_TIMESTAMP,'intent',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", "23514", "ck_zip_build_fence").await;
        rejects_sql(&db, "INSERT INTO zip_root_references (batch_id,revision,epoch,node_identity,tier,cid,created_at,updated_at) VALUES ('batch',1,1,'node','hot','root',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", "23503", "fk_zip_root_intent").await;
        let snapshot = zip::snapshot(&db, "batch").await.unwrap().unwrap();
        assert_eq!(snapshot.batch.state, "open");
        assert_eq!(snapshot.batch.root_status, "pending");
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].cid.as_deref(), Some("leaf"));
        assert!(snapshot.builds.is_empty() && snapshot.references.is_empty());
    }).await;
}

#[tokio::test]
#[ignore = "NOT RUN by default: requires IPFS_S3_TEST_POSTGRES_URL to PG17"]
async fn pg17_same_token_concurrent_admission_returns_one_immutable_batch() {
    isolated(true, |a, b| async move {
        store::bucket::create(&a, "bucket", None).await.unwrap();
        let first_request = admission("first");
        let second_request = admission("second");
        let (first, second) = tokio::time::timeout(Duration::from_secs(8), async {
            tokio::join!(zip::admit(&a, &first_request), zip::admit(&b, &second_request))
        }).await.unwrap();
        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(first.id, second.id, "shared token must not create two batches");
        let mut mismatched = admission("third");
        mismatched.fingerprint = "different".into();
        assert!(matches!(zip::admit(&b, &mismatched).await.unwrap_err(),
            AppError::InvalidZipParameter(ref message) if message == "ZIP idempotency token conflict"));
        rejects_sql(&a, "INSERT INTO zip_batches (id,owner,source,token,fingerprint,bucket,archive_key,input_identity,captured_options,created_at,updated_at) VALUES ('duplicate','owner','direct','token','fingerprint','bucket','archive','digest','{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", "23505", "uq_zip_batch_intent").await;
        let rows = a.query_all(Statement::from_string(DatabaseBackend::Postgres,
            "SELECT id FROM zip_batches WHERE owner='owner' AND source='direct' AND token='token'"
        )).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].try_get::<String>("", "id").unwrap(), first.id);
    }).await;
}

#[tokio::test]
#[ignore = "NOT RUN by default: requires IPFS_S3_TEST_POSTGRES_URL to PG17"]
async fn pg17_adopted_root_requires_receipt_and_has_one_owner() {
    isolated(true, |db, _| async move {
        prepared_output(&db).await;
        let claim = zip::claim_root(&db, "batch", "worker", 60).await.unwrap();
        zip::mark_invoked(&db, &claim).await.unwrap();
        zip::retain_candidate(&db, &claim, "node", "hot", "root").await.unwrap();
        rejects_sql(&db, "UPDATE zip_root_references SET state='adopted' WHERE batch_id='batch' AND cid='root'", "23514", "ck_zip_root_adopted").await;
        zip::verify_root(&db, &claim, "node", "hot", "root", "verified-recursive-pin").await.unwrap();
        zip::retain_candidate(&db, &claim, "other-node", "hot", "other-root").await.unwrap();
        db.execute_unprepared("UPDATE zip_root_references SET state='adopted' WHERE batch_id='batch' AND cid='root'").await.unwrap();
        db.execute_unprepared("UPDATE zip_root_references SET verification_receipt='independently-verified' WHERE batch_id='batch' AND cid='other-root'").await.unwrap();
        rejects_sql(&db, "UPDATE zip_root_references SET state='adopted' WHERE batch_id='batch' AND cid='other-root'", "23505", "uq_zip_root_adopted").await;
        assert_eq!(zip::root_existence(&db, "batch", "node", "hot", "root").await.unwrap(), "adopted");
        assert_eq!(zip::root_existence(&db, "batch", "other-node", "hot", "other-root").await.unwrap(), "retained");
    }).await;
}

#[tokio::test]
#[ignore = "NOT RUN by default: requires IPFS_S3_TEST_POSTGRES_URL to PG17"]
async fn concurrent_claim_and_close_serializes_and_fences_old_epoch() {
    isolated(true, |a,b| async move {
        let admission = BatchAdmission { id:"batch".into(), owner:"owner".into(), source:"direct".into(),
            token:"token".into(), fingerprint:"fingerprint".into(), bucket:"bucket".into(),
            archive_key:"archive".into(), input_identity:"digest".into(), captured_options:"{}".into() };
        store::bucket::create(&a,"bucket",None).await.unwrap();
        zip::admit(&a,&admission).await.unwrap();
        zip::prepare_manifest(&a,"batch",&[ManifestItem::Success {
            path:"a".into(),object_key:"out/a".into(),cid:"leaf".into(),size:1,
        }]).await.unwrap();
        let (left,right) = tokio::time::timeout(Duration::from_secs(8),async {
            tokio::join!(zip::claim_root(&a,"batch","a",30),zip::claim_root(&b,"batch","b",30))
        }).await.unwrap();
        let old = match (left, right) {
            (Ok(claim), Err(error)) | (Err(error), Ok(claim)) => {
                assert_stale(error);
                claim
            }
            (left, right) => panic!("expected one claim and one stale loser: {left:?}, {right:?}"),
        };
        zip::mark_invoked(&a,&old).await.unwrap();
        zip::mark_unknown(&a,&old).await.unwrap();
        let claim = zip::claim_root(&b,"batch","successor",30).await.unwrap();
        zip::retain_candidate(&a,&old,"node","hot","root").await.unwrap();
        assert_stale(zip::verify_root(&a,&old,"node","hot","root","stale-receipt").await.unwrap_err());
        zip::mark_reconciling(&b,&claim).await.unwrap();
        zip::retain_candidate(&b,&claim,"node","hot","root").await.unwrap();
        zip::verify_root(&b,&claim,"node","hot","root","verified-recursive-pin").await.unwrap();
        a.execute_unprepared("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('obj','bucket','out/a','leaf',1,'leaf')").await.unwrap();
        a.execute_unprepared("INSERT INTO object_versions (id,bucket,key,kind,object_id,sequence,is_latest,created_at,updated_at,lifecycle_age_started_at) VALUES ('v1','bucket','out/a','object','obj',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
        let (first,second) = tokio::time::timeout(Duration::from_secs(8),async {
            tokio::join!(close(&a,claim.clone()),close(&b,claim.clone()))
        }).await.unwrap();
        match (first, second) {
            (Ok(()), Err(error)) | (Err(error), Ok(())) => {
                assert_eq!(error, "internal error: stale ZIP batch ownership or root claim");
            }
            (first, second) => panic!("expected one close and one stale loser: {first:?}, {second:?}"),
        }
        assert_eq!(zip::root_existence(&a,"batch","node","hot","root").await.unwrap(),"adopted");
        let result = zip::snapshot(&a,"batch").await.unwrap().unwrap();
        assert_eq!(result.entries[0].version_row_id.as_deref(),Some("v1"));
        assert_eq!(result.references.iter().filter(|r|r.state=="retained").count(),1);
        assert!(result.references.iter().any(|r| r.epoch == old.epoch && r.state == "retained"
            && r.verification_receipt.is_none()));
    }).await;
}

#[tokio::test]
#[ignore = "NOT RUN by default: requires IPFS_S3_TEST_POSTGRES_URL to PG17"]
async fn published_failed_claim_waits_for_lease_and_fences_old_outcomes() {
    isolated(true, |a, b| async move {
        store::bucket::create(&a, "bucket", None).await.unwrap();
        zip::admit(&a, &BatchAdmission {
            id: "batch".into(), owner: "owner".into(), source: "direct".into(),
            token: "token".into(), fingerprint: "fingerprint".into(), bucket: "bucket".into(),
            archive_key: "archive".into(), input_identity: "digest".into(), captured_options: "{}".into(),
        }).await.unwrap();
        zip::prepare_manifest(&a, "batch", &[ManifestItem::Success {
            path: "a".into(), object_key: "out/a".into(), cid: "leaf".into(), size: 1,
        }]).await.unwrap();
        a.execute_unprepared("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('obj','bucket','out/a','leaf',1,'leaf')").await.unwrap();
        a.execute_unprepared("INSERT INTO object_versions (id,bucket,key,kind,object_id,sequence,is_latest,created_at,updated_at,lifecycle_age_started_at) VALUES ('v1','bucket','out/a','object','obj',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();

        let old = zip::claim_root(&a, "batch", "worker-a", 60).await.unwrap();
        zip::mark_invoked(&a, &old).await.unwrap();
        let tx = a.begin().await.unwrap();
        old.publish_failed(&tx, &[VersionBinding {
            path: "a".into(), version_row_id: "v1".into(),
        }], false, "{\"attempt\":\"initial\"}", "root_build_failed").await.unwrap();
        tx.commit().await.unwrap();
        assert_stale(zip::claim_root(&b, "batch", "worker-b", 60).await.unwrap_err());
        let tx = b.begin().await.unwrap();
        assert_stale(zip::settle_root_retry(&tx, "batch", "{\"attempt\":\"unclaimed\"}",
            RootOutcome::Failed { code: "root_build_failed" }).await.unwrap_err());
        tx.rollback().await.unwrap();

        a.execute_unprepared("UPDATE zip_root_builds SET lease_until='2000-01-01T00:00:00Z' WHERE batch_id='batch'").await.unwrap();
        let next = zip::claim_root(&b, "batch", "worker-b", 60).await.unwrap();
        assert!(next.revision > old.revision && next.epoch > old.epoch);
        // The current key can move after publication; a retry must retain v1.
        a.execute_unprepared("UPDATE objects SET is_latest=FALSE WHERE id='obj'").await.unwrap();
        a.execute_unprepared("UPDATE object_versions SET is_latest=FALSE WHERE id='v1'").await.unwrap();
        a.execute_unprepared("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('obj2','bucket','out/a','replacement',11,'replacement')").await.unwrap();
        a.execute_unprepared("INSERT INTO object_versions (id,bucket,key,version_id,kind,object_id,sequence,is_latest,created_at,updated_at,lifecycle_age_started_at) VALUES ('v2','bucket','out/a','new','object','obj2',2,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
        let latest = a.query_one(Statement::from_string(DatabaseBackend::Postgres,
            "SELECT id FROM object_versions WHERE bucket='bucket' AND key='out/a' AND is_latest=TRUE"
        )).await.unwrap().unwrap();
        assert_eq!(latest.try_get::<String>("", "id").unwrap(), "v2");
        zip::retain_candidate(&a, &old, "node", "hot", "old-root").await.unwrap();
        assert_stale(zip::verify_root(&a, &old, "node", "hot", "old-root", "old-receipt")
            .await.unwrap_err());
        let tx = a.begin().await.unwrap();
        assert_stale(old.settle_failed_retry(&tx, "{\"attempt\":\"stale\"}", "root_build_failed")
            .await.unwrap_err());
        tx.rollback().await.unwrap();
        let tx = a.begin().await.unwrap();
        assert_stale(zip::settle_root_retry(&tx, "batch", "{\"attempt\":\"stale\"}", RootOutcome::Verified {
            claim: old.clone(), node_identity: "node".into(), tier: "hot".into(), cid: "old-root".into(),
        }).await.unwrap_err());
        tx.rollback().await.unwrap();
        let snapshot = zip::snapshot(&b, "batch").await.unwrap().unwrap();
        assert_eq!(snapshot.batch.terminal_result.as_deref(), Some("{\"attempt\":\"initial\"}"));
        assert_eq!(snapshot.batch.root_revision, next.revision);
        assert_eq!(snapshot.entries[0].version_row_id.as_deref(), Some("v1"));
        assert!(snapshot.references.iter().any(|r| r.epoch == old.epoch
            && r.cid == "old-root" && r.state == "retained" && r.verification_receipt.is_none()));

        let tx = b.begin().await.unwrap();
        next.settle_failed_retry(&tx, "{\"attempt\":\"retry\"}", "root_build_failed").await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(zip::read(&a, "batch").await.unwrap().unwrap().terminal_result.as_deref(),
            Some("{\"attempt\":\"retry\"}"));
        let snapshot = zip::snapshot(&a, "batch").await.unwrap().unwrap();
        assert_eq!(snapshot.entries[0].version_row_id.as_deref(), Some("v1"));
        assert!(snapshot.references.iter().any(|r| r.cid == "old-root" && r.state == "retained"));
    }).await;
}

struct Previous;
impl MigratorTrait for Previous {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        use store::migrations::*;
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
            Box::new(m20260920_000003_pin_extension_decision::Migration),
            Box::new(m20260920_000004_multipart_pin_decision::Migration),
            Box::new(m20260920_000005_import_pin_decision::Migration),
            Box::new(m20260920_000006_pin_submit_correlation::Migration),
        ]
    }
}

#[tokio::test]
#[ignore = "NOT RUN by default: requires IPFS_S3_TEST_POSTGRES_URL to PG17"]
async fn previous_pg_schema_upgrades_without_rewriting_objects_and_rejects_down() {
    isolated(false, |db,_other| async move {
        Previous::up(&db,None).await.unwrap();
        store::bucket::create(&db,"bucket",None).await.unwrap();
        db.execute_unprepared("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('old','bucket','archive','legacy',1,'legacy')").await.unwrap();
        assert!(db.query_one(Statement::from_string(DatabaseBackend::Postgres,
            "SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='import_jobs' AND column_name='root_capture_json'"
        )).await.unwrap().is_none());
        store::run_migrations(&db).await.unwrap();
        let capture = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
            "SELECT data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='import_jobs' AND column_name='root_capture_json'"
        )).await.unwrap().unwrap();
        assert_eq!(capture.try_get::<String>("", "data_type").unwrap(), "text");
        assert_eq!(capture.try_get::<String>("", "is_nullable").unwrap(), "YES");
        assert_eq!(capture.try_get::<Option<String>>("", "column_default").unwrap(), None);
        zip::admit(&db,&BatchAdmission { id:"batch".into(), owner:"principal".into(), source:"direct".into(),
            token:"token".into(), fingerprint:"fingerprint".into(), bucket:"bucket".into(),
            archive_key:"archive".into(), input_identity:"digest".into(), captured_options:"{}".into() }).await.unwrap();
        let row = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
            "SELECT cid FROM objects WHERE id='old'")).await.unwrap().unwrap();
        assert_eq!(row.try_get::<String>("","cid").unwrap(),"legacy");
        let err = store::migrations::m20260927_000001_zip_batches::Migration
            .down(&SchemaManager::new(&db)).await.unwrap_err();
        assert!(matches!(err, DbErr::Migration(ref message)
            if message == "ZIP batch downgrade would erase durable owner/recovery evidence"));
        let err = store::migrations::m20260927_000002_import_zip_root_capture::Migration
            .down(&SchemaManager::new(&db)).await.unwrap_err();
        assert!(matches!(err, DbErr::Migration(ref message)
            if message == "import ZIP root capture cannot be downgraded without losing admission evidence"));
        assert!(zip::snapshot(&db,"batch").await.unwrap().is_some());
    }).await;
}

async fn close(db: &DatabaseConnection, claim: zip::RootClaim) -> Result<(), String> {
    let tx = db.begin().await.map_err(|e| e.to_string())?;
    let result = zip::publish(
        &tx,
        "batch",
        &[VersionBinding {
            path: "a".into(),
            version_row_id: "v1".into(),
        }],
        false,
        "{}",
        RootOutcome::Verified {
            claim,
            node_identity: "node".into(),
            tier: "hot".into(),
            cid: "root".into(),
        },
    )
    .await;
    match result {
        Ok(()) => tx.commit().await.map_err(|e| e.to_string()),
        Err(error) => {
            tx.rollback().await.map_err(|e| e.to_string())?;
            Err(error.to_string())
        }
    }
}
