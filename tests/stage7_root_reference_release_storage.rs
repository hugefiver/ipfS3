//! Logical root activity is independent of durable adoption and terminal replay.
#[allow(dead_code)]
mod support;

use ipfs_s3_gateway::store::{self, zip};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, Statement, TransactionTrait};
use sea_orm_migration::{MigrationTrait, SchemaManager};

#[path = "support/stage7_root_reference_release_http.rs"]
mod root_http;

async fn setup() -> DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    db
}

async fn prepared(db: &DatabaseConnection) -> zip::RootClaim {
    store::bucket::create(db, "bucket", None).await.unwrap();
    zip::admit(
        db,
        &zip::BatchAdmission {
            id: "batch".into(),
            owner: "principal".into(),
            source: "direct".into(),
            token: "token".into(),
            fingerprint: "fingerprint".into(),
            bucket: "bucket".into(),
            archive_key: "archive.zip".into(),
            input_identity: "digest".into(),
            captured_options: "{}".into(),
        },
    )
    .await
    .unwrap();
    zip::prepare_manifest(
        db,
        "batch",
        &[zip::ManifestItem::Success {
            path: "file".into(),
            object_key: "out/file".into(),
            cid: "leaf".into(),
            size: 1,
        }],
    )
    .await
    .unwrap();
    db.execute_unprepared("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('obj','bucket','out/file','leaf',1,'leaf')").await.unwrap();
    db.execute_unprepared("INSERT INTO object_versions (id,bucket,key,kind,object_id,sequence,is_latest,created_at,updated_at,lifecycle_age_started_at) VALUES ('version','bucket','out/file','object','obj',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    let claim = zip::claim_root(db, "batch", "worker", 60).await.unwrap();
    zip::mark_invoked(db, &claim).await.unwrap();
    zip::retain_candidate(db, &claim, "node-a", "hot", "root")
        .await
        .unwrap();
    claim
}

// The management action owns authorization, exact CAS and its receipt. Storage
// consumer tests set the marker at that boundary without duplicating the action.
async fn mark_released(db: &DatabaseConnection) {
    db.execute_unprepared("UPDATE zip_root_references SET logical_released_at='2026-10-13T01:02:03.456789Z' WHERE batch_id='batch' AND revision=1 AND epoch=1 AND node_identity='node-a' AND tier='hot' AND cid='root'").await.unwrap();
}

#[tokio::test]
async fn released_candidate_is_not_verified_and_same_key_late_return_does_not_revive_it() {
    let db = setup().await;
    let claim = prepared(&db).await;
    mark_released(&db).await;
    zip::retain_candidate(&db, &claim, "node-a", "hot", "root")
        .await
        .unwrap();
    assert_eq!(
        zip::root_existence(&db, "batch", "node-a", "hot", "root")
            .await
            .unwrap(),
        "absent"
    );
    assert!(
        zip::verify_root(&db, &claim, "node-a", "hot", "root", "verified-proof")
            .await
            .is_err()
    );
    let snapshot = zip::snapshot(&db, "batch").await.unwrap().unwrap();
    assert_eq!(snapshot.references.len(), 1);
    assert_eq!(snapshot.references[0].state, "retained");
    assert!(snapshot.references[0].verification_receipt.is_none());
    assert_eq!(snapshot.builds[0].status, "invoked");
    assert!(snapshot.references[0].logical_released_at.is_some());
}

#[tokio::test]
async fn released_verified_candidate_cannot_be_adopted_or_bind_output_version() {
    let db = setup().await;
    let claim = prepared(&db).await;
    zip::verify_root(&db, &claim, "node-a", "hot", "root", "verified-proof")
        .await
        .unwrap();
    mark_released(&db).await;
    let tx = db.begin().await.unwrap();
    assert!(
        zip::publish(
            &tx,
            "batch",
            &[zip::VersionBinding {
                path: "file".into(),
                version_row_id: "version".into()
            }],
            false,
            "{}",
            zip::RootOutcome::Verified {
                claim,
                node_identity: "node-a".into(),
                tier: "hot".into(),
                cid: "root".into(),
            }
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    let snapshot = zip::snapshot(&db, "batch").await.unwrap().unwrap();
    assert_eq!(snapshot.batch.state, "open");
    assert!(snapshot.entries[0].version_row_id.is_none());
    assert_eq!(snapshot.references[0].state, "retained");
    assert_eq!(
        snapshot.references[0].verification_receipt.as_deref(),
        Some("verified-proof")
    );
}

async fn native_root_rows(db: &DatabaseConnection) -> Vec<Vec<Option<String>>> {
    let fields = [
        "batch_id",
        "revision",
        "epoch",
        "node_identity",
        "tier",
        "cid",
        "state",
        "verification_receipt",
        "created_at",
        "updated_at",
    ];
    let projection = fields
        .iter()
        .map(|field| format!("CAST({field} AS TEXT) AS {field}"))
        .collect::<Vec<_>>()
        .join(",");
    db.query_all(Statement::from_string(db.get_database_backend(), format!(
        "SELECT {projection} FROM zip_root_references ORDER BY batch_id,revision,epoch,node_identity,tier,cid"
    ))).await.unwrap().iter().map(|row| fields.iter().map(|field| row.try_get("", field).unwrap()).collect()).collect()
}

async fn migration_roundtrip(db: &DatabaseConnection) {
    use store::migrations::m20261013_000001_zip_root_logical_release::Migration;
    let claim = prepared(db).await;
    zip::verify_root(db, &claim, "node-a", "hot", "root", "verified-proof")
        .await
        .unwrap();
    let before = native_root_rows(db).await;
    let manager = SchemaManager::new(db);
    Migration.down(&manager).await.unwrap();
    assert_eq!(native_root_rows(db).await, before);
    db.execute_unprepared(
        "DELETE FROM seaql_migrations WHERE version='m20261013_000001_zip_root_logical_release'",
    )
    .await
    .unwrap();
    store::run_migrations(db).await.unwrap();
    assert_eq!(native_root_rows(db).await, before);
    let frame = store::pinning::management::native::capture(db)
        .await
        .unwrap();
    frame.validate().unwrap();
    let root = frame.table("zip_root_references").unwrap();
    assert_eq!(root.rows[0].get("logical_released_at"), Some(&None));
    let marker = root
        .definitions
        .iter()
        .find(|column| column.name == "logical_released_at")
        .unwrap();
    assert!(marker.nullable);
    assert_eq!(marker.sqlite_type, "TEXT");
    assert_eq!(marker.postgres_type, "timestamp with time zone");
    for name in ["created_at", "updated_at"] {
        let time = root
            .definitions
            .iter()
            .find(|column| column.name == name)
            .unwrap();
        assert!(!time.nullable);
        assert_eq!(time.sqlite_type, "TIMESTAMP");
        assert_eq!(time.postgres_type, "timestamp with time zone");
    }
    mark_released(db).await;
    let marked = store::pinning::management::native::capture(db)
        .await
        .unwrap();
    assert!(marked.table("zip_root_references").unwrap().rows[0]["logical_released_at"].is_some());
    assert!(Migration.down(&manager).await.is_err());
    assert_eq!(native_root_rows(db).await, before);
    assert!(
        store::pinning::management::native::capture(db)
            .await
            .unwrap()
            == marked
    );
}

#[tokio::test]
async fn sqlite_upgrade_and_safe_downgrade_preserve_native_rows_and_release_barrier() {
    migration_roundtrip(&setup().await).await;
}

#[tokio::test]
#[ignore = "requires a parent-owned fresh PostgreSQL root schema"]
async fn postgres_upgrade_and_safe_downgrade_preserve_native_rows_and_release_barrier() {
    let url = std::env::var("IPFS_S3_STAGE7_ROOT_DATABASE_URL")
        .expect("parent must supply a fresh exclusive PostgreSQL schema");
    let parsed = url::Url::parse(&url).unwrap();
    assert!(matches!(parsed.scheme(), "postgres" | "postgresql"));
    assert!(matches!(
        parsed.host_str(),
        Some("127.0.0.1" | "localhost" | "[::1]")
    ));
    let mut options = sea_orm::ConnectOptions::new(url);
    options.max_connections(1).sqlx_logging(false);
    let db = Database::connect(options).await.unwrap();
    let scope = db.query_one(Statement::from_string(db.get_database_backend(),
        "SELECT current_schema() AS name, current_schemas(false)=ARRAY[current_schema()]::name[] AS exclusive,
         n.nspowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) AS owned,
         NOT EXISTS (SELECT 1 FROM pg_class WHERE relnamespace=n.oid)
         AND NOT EXISTS (SELECT 1 FROM pg_proc WHERE pronamespace=n.oid)
         AND NOT EXISTS (SELECT 1 FROM pg_type WHERE typnamespace=n.oid) AS empty
         FROM pg_namespace n WHERE n.nspname=current_schema()"
    )).await.unwrap().unwrap();
    let name: String = scope.try_get("", "name").unwrap();
    assert!(name.starts_with("stage7_root_") && name.len() <= 63);
    for flag in ["exclusive", "owned", "empty"] {
        assert!(
            scope.try_get::<bool>("", flag).unwrap(),
            "parent scope must be {flag}"
        );
    }
    db.execute_unprepared("SET TIME ZONE 'UTC'").await.unwrap();
    db.execute_unprepared("SET statement_timeout='8s'")
        .await
        .unwrap();
    store::run_migrations(&db).await.unwrap();
    migration_roundtrip(&db).await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn shared_node_and_epoch_references_stay_active_and_all_history_is_retained() {
    let db = setup().await;
    let old = prepared(&db).await;
    zip::retain_candidate(&db, &old, "node-b", "hot", "root")
        .await
        .unwrap();
    zip::mark_unknown(&db, &old).await.unwrap();
    let next = zip::claim_root(&db, "batch", "successor", 60)
        .await
        .unwrap();
    zip::mark_invoked(&db, &next).await.unwrap();
    zip::retain_candidate(&db, &next, "node-a", "hot", "root")
        .await
        .unwrap();
    mark_released(&db).await;
    zip::retain_candidate(&db, &old, "node-a", "hot", "root")
        .await
        .unwrap();
    assert_eq!(
        zip::root_existence(&db, "batch", "node-b", "hot", "root")
            .await
            .unwrap(),
        "retained"
    );
    assert_eq!(
        zip::root_existence(&db, "batch", "node-a", "hot", "root")
            .await
            .unwrap(),
        "retained"
    );
    zip::verify_root(&db, &next, "node-a", "hot", "root", "successor-proof")
        .await
        .unwrap();
    let snapshot = zip::snapshot(&db, "batch").await.unwrap().unwrap();
    assert_eq!(snapshot.references.len(), 3);
    let released = snapshot
        .references
        .iter()
        .filter(|row| row.logical_released_at.is_some())
        .collect::<Vec<_>>();
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].epoch, old.epoch);
    assert_eq!(released[0].node_identity, "node-a");
    assert!(snapshot.references.iter().any(|row| row.epoch == next.epoch
        && row.verification_receipt.as_deref() == Some("successor-proof")
        && row.logical_released_at.is_none()));
}

#[tokio::test]
async fn released_adoption_keeps_terminal_history_and_cannot_reopen_terminal_batch() {
    for terminal_status in ["complete", "partial"] {
        let db = setup().await;
        let claim = prepared(&db).await;
        if terminal_status == "partial" {
            db.execute_unprepared("INSERT INTO zip_manifest_entries (batch_id,path,error_code,created_at) VALUES ('batch','failed-file','invalid_entry',CURRENT_TIMESTAMP)").await.unwrap();
        }
        zip::verify_root(&db, &claim, "node-a", "hot", "root", "verified-proof")
            .await
            .unwrap();
        let tx = db.begin().await.unwrap();
        zip::publish(
            &tx,
            "batch",
            &[zip::VersionBinding {
                path: "file".into(),
                version_row_id: "version".into(),
            }],
            false,
            "{\"root_cid\":\"root\"}",
            zip::RootOutcome::Verified {
                claim: claim.clone(),
                node_identity: "node-a".into(),
                tier: "hot".into(),
                cid: "root".into(),
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let before = zip::snapshot(&db, "batch").await.unwrap().unwrap();
        assert_eq!(before.batch.root_status, terminal_status);
        mark_released(&db).await;
        zip::retain_candidate(&db, &claim, "node-a", "hot", "root")
            .await
            .unwrap();
        assert_eq!(
            zip::root_existence(&db, "batch", "node-a", "hot", "root")
                .await
                .unwrap(),
            "absent"
        );
        assert!(zip::claim_root(&db, "batch", "retry", 60).await.is_err());
        let tx = db.begin().await.unwrap();
        assert!(
            zip::settle_root_retry(
                &tx,
                "batch",
                "{}",
                zip::RootOutcome::Failed {
                    code: "root_build_failed"
                }
            )
            .await
            .is_err()
        );
        tx.rollback().await.unwrap();
        let after = zip::snapshot(&db, "batch").await.unwrap().unwrap();
        assert_eq!(after.batch, before.batch);
        assert_eq!(after.entries, before.entries);
        assert_eq!(after.builds, before.builds);
        assert_eq!(after.references[0].state, "adopted");
        assert_eq!(
            after.references[0].verification_receipt,
            before.references[0].verification_receipt
        );
        assert!(after.references[0].logical_released_at.is_some());
    }
}

#[tokio::test]
async fn released_direct_zip_replays_committed_root_without_new_rpc() {
    root_http::direct_replay().await;
}

#[tokio::test]
async fn released_multipart_zip_replays_committed_root_without_new_rpc() {
    root_http::multipart_replay().await;
}
