//! Explicit opt-in only; owns a fresh UUID schema in an authorized PG17 test DB.
//! IPFS_S3_TEST_POSTGRES_URL + --ignored. No production or account access.
mod stage9_zip_support;
use futures_util::FutureExt;
use ipfs_s3_gateway::store::{
    self,
    entities::{object_version, pin_lease, pin_provider_usage},
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, EntityTrait, PaginatorTrait,
    Statement,
};
use stage9_zip_support::*;
use std::panic::AssertUnwindSafe;

#[tokio::test]
#[ignore = "requires explicitly authorized isolated PostgreSQL 17"]
async fn r25_pg_tail_rollback_then_exact_shared_cid_publication() {
    // Same fixture on both engines; SQLite is not used as PG transaction evidence.
    let sqlite = setup().await;
    sqlite.close().await.unwrap();
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL").expect("set authorized isolated PG17 URL");
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
    assert!(version.starts_with("17."));
    let schema = format!("r25_zip_store_{}", uuid::Uuid::new_v4().simple());
    assert!(
        schema
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    );
    let exists = || {
        Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT 1 FROM pg_namespace WHERE nspname=$1",
            [schema.clone().into()],
        )
    };
    assert!(admin.query_one(exists()).await.unwrap().is_none());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let mut owned_connection = None;
    let result = AssertUnwindSafe(async {
        let mut options = ConnectOptions::new(&url);
        options.min_connections(1).max_connections(1);
        let db = Database::connect(options).await.unwrap();
        owned_connection = Some(db.clone());
        db.execute_unprepared(&format!("SET search_path TO {schema}")).await.unwrap();
        db.execute_unprepared("SET statement_timeout TO '8s'").await.unwrap();
        initialize(&db).await;
        let config = config();
        let (request, batch, capture, guard) = prepared(&db, &config, vec![]).await;
        let common = ipfs_s3_gateway::store::pinning::publication::CapturedZipPublication {
            batch_id: batch.batch_id.clone(), source: Some(request.archive.object.clone()), source_tags: request.archive.tags.clone(),
            manifest: ipfs_s3_gateway::zip::batch::FinalZipManifest { successful: request.entries.iter().map(|o|
                ipfs_s3_gateway::zip::batch::ManifestFile { relative_path: o.key.trim_start_matches("out/").into(), object_key: o.key.clone(), cid: o.cid.clone(), size: o.logical_size }).collect(), ..Default::default() },
            successes: request.entries.clone(), capture: capture.clone(),
            authority: ipfs_s3_gateway::store::pinning::publication::ZipPublicationAuthority::Legacy(
                Box::new(ipfs_s3_gateway::store::pinning::publication::LegacyZipAuthority {
                    standard_guard: Some(guard.clone()), upload_target: None, import_guard: None, result_rows: vec![], replay: None })),
            root_outcome: batch.root_outcome.clone(), terminal_result: batch.terminal_result.clone() };
        db.execute_unprepared("CREATE FUNCTION r25_tail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state='published' THEN RAISE EXCEPTION 'r25 PG tail'; END IF; RETURN NEW; END $$").await.unwrap();
        db.execute_unprepared("CREATE TRIGGER r25_tail BEFORE UPDATE ON zip_batches FOR EACH ROW EXECUTE FUNCTION r25_tail()").await.unwrap();
        assert!(ipfs_s3_gateway::store::pinning::publication::publish_captured_zip(&db, common.clone()).await.unwrap_err().to_string().contains("r25 PG tail"));
        assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(pin_lease::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(pin_provider_usage::Entity::find().count(&db).await.unwrap(), 0);
        let snapshot = store::zip::snapshot(&db, "batch").await.unwrap().unwrap();
        assert_eq!(snapshot.batch.state, "open");
        assert!(snapshot.entries.iter().all(|e| e.version_row_id.is_none()));
        db.execute_unprepared("DROP TRIGGER r25_tail ON zip_batches").await.unwrap();
        // Known rollback keeps the claim; retry the same exact captured request.
        ipfs_s3_gateway::store::pinning::publication::publish_captured_zip(&db, common).await.unwrap();
        assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 3);
        assert_eq!(pin_lease::Entity::find().count(&db).await.unwrap(), 2);
        let usage = pin_provider_usage::Entity::find().one(&db).await.unwrap().unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (5, 1));
        assert!(publish(&db, &config, request, batch, &capture, guard).await.is_err());
        assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 3);
    }).catch_unwind().await;
    if let Some(db) = owned_connection {
        db.close().await.unwrap();
    }
    // Exact generated schema only; assertion precedes the recursive SQL cleanup.
    assert!(schema.starts_with("r25_zip_store_") && schema.len() == 46);
    admin
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    assert!(admin.query_one(exists()).await.unwrap().is_none());
    admin.close().await.unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
