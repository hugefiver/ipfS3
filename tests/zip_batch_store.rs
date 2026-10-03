use ipfs_s3_gateway::store::{
    self,
    zip::{self, BatchAdmission, ManifestItem, RootOutcome, VersionBinding},
};
use sea_orm::{ConnectionTrait, Database, TransactionTrait};
use sea_orm_migration::{MigrationTrait, SchemaManager};

async fn setup() -> sea_orm::DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .unwrap();
    store::run_migrations(&db).await.unwrap();
    db
}

async fn file_connections() -> (
    tempfile::TempDir,
    sea_orm::DatabaseConnection,
    sea_orm::DatabaseConnection,
) {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("zip-claims.db")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let a = store::connect_database(&url).await.unwrap();
    store::run_migrations(&a).await.unwrap();
    let b = store::connect_database(&url).await.unwrap();
    (directory, a, b)
}

async fn prepared_output(db: &sea_orm::DatabaseConnection) {
    store::bucket::create(db, "bucket", None).await.unwrap();
    zip::admit(db, &admission("one")).await.unwrap();
    zip::prepare_manifest(
        db,
        "one",
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
async fn live_claim_fences_failed_publication_and_expired_claim_cannot_settle_retry() {
    let (_directory, a, b) = file_connections().await;
    prepared_output(&a).await;
    let old = zip::claim_root(&a, "one", "worker-a", 60).await.unwrap();
    zip::mark_invoked(&a, &old).await.unwrap();
    assert!(zip::claim_root(&b, "one", "worker-b", 60).await.is_err());

    let tx = b.begin().await.unwrap();
    assert!(
        zip::publish(
            &tx,
            "one",
            &[VersionBinding {
                path: "a".into(),
                version_row_id: "v1".into()
            }],
            false,
            "{\"attempt\":\"unclaimed\"}",
            RootOutcome::Failed {
                code: "path_conflict"
            }
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();

    let tx = a.begin().await.unwrap();
    old.publish_failed(
        &tx,
        &[VersionBinding {
            path: "a".into(),
            version_row_id: "v1".into(),
        }],
        false,
        "{\"attempt\":\"initial\"}",
        "root_build_failed",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert!(
        zip::claim_root(&b, "one", "worker-b", 60).await.is_err(),
        "published/failed still owns a live lease"
    );

    let tx = b.begin().await.unwrap();
    assert!(
        zip::settle_root_retry(
            &tx,
            "one",
            "{\"attempt\":\"unclaimed\"}",
            RootOutcome::Failed {
                code: "root_build_failed"
            }
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();

    a.execute_unprepared(
        "UPDATE zip_root_builds SET lease_until='2000-01-01T00:00:00Z' WHERE batch_id='one'",
    )
    .await
    .unwrap();
    let next = zip::claim_root(&b, "one", "worker-b", 60).await.unwrap();
    assert!(next.revision > old.revision);
    assert!(next.epoch > old.epoch);
    assert!(
        zip::mark_unknown(&a, &old).await.is_err(),
        "old worker cannot release the successor's lease"
    );

    let tx = a.begin().await.unwrap();
    assert!(
        old.settle_failed_retry(&tx, "{\"attempt\":\"stale\"}", "root_build_failed")
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    let tx = a.begin().await.unwrap();
    assert!(
        zip::settle_root_retry(
            &tx,
            "one",
            "{\"attempt\":\"stale\"}",
            RootOutcome::Verified {
                claim: old.clone(),
                node_identity: "node".into(),
                tier: "hot".into(),
                cid: "old".into()
            }
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    let snapshot = zip::snapshot(&b, "one").await.unwrap().unwrap();
    assert_eq!(
        snapshot.batch.terminal_result.as_deref(),
        Some("{\"attempt\":\"initial\"}")
    );
    assert_eq!(snapshot.batch.root_revision, next.revision);
    assert_eq!(snapshot.builds.last().unwrap().status, "intent");

    let tx = b.begin().await.unwrap();
    next.settle_failed_retry(&tx, "{\"attempt\":\"retry-failed\"}", "root_build_failed")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let tx = a.begin().await.unwrap();
    assert!(
        old.settle_failed_retry(&tx, "{\"attempt\":\"stale-after-b\"}", "root_build_failed")
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        zip::read(&a, "one")
            .await
            .unwrap()
            .unwrap()
            .terminal_result
            .as_deref(),
        Some("{\"attempt\":\"retry-failed\"}")
    );

    zip::mark_invoked(&b, &next).await.unwrap();
    zip::retain_candidate(&b, &next, "node", "hot", "new")
        .await
        .unwrap();
    zip::verify_root(&b, &next, "node", "hot", "new", "durable-receipt")
        .await
        .unwrap();
    let tx = b.begin().await.unwrap();
    zip::settle_root_retry(
        &tx,
        "one",
        "{\"attempt\":\"verified\"}",
        RootOutcome::Verified {
            claim: next,
            node_identity: "node".into(),
            tier: "hot".into(),
            cid: "new".into(),
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let snapshot = zip::snapshot(&a, "one").await.unwrap().unwrap();
    assert_eq!(snapshot.batch.root_status, "complete");
    assert_eq!(snapshot.batch.root_cid.as_deref(), Some("new"));
    assert_eq!(
        snapshot.batch.terminal_result.as_deref(),
        Some("{\"attempt\":\"verified\"}")
    );
    assert_eq!(
        snapshot.references[0].verification_receipt.as_deref(),
        Some("durable-receipt")
    );
    assert_eq!(snapshot.references[0].state, "adopted");
}

#[tokio::test]
async fn verified_old_receipt_remains_retained_after_expired_takeover() {
    let (_directory, a, b) = file_connections().await;
    prepared_output(&a).await;
    let old = zip::claim_root(&a, "one", "worker-a", 60).await.unwrap();
    zip::mark_invoked(&a, &old).await.unwrap();
    zip::retain_candidate(&a, &old, "node", "hot", "old-root")
        .await
        .unwrap();
    zip::verify_root(&a, &old, "node", "hot", "old-root", "old-receipt")
        .await
        .unwrap();
    let tx = a.begin().await.unwrap();
    old.publish_failed(
        &tx,
        &[VersionBinding {
            path: "a".into(),
            version_row_id: "v1".into(),
        }],
        false,
        "{\"attempt\":\"initial\"}",
        "root_build_failed",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    a.execute_unprepared(
        "UPDATE zip_root_builds SET lease_until='2000-01-01T00:00:00Z' WHERE batch_id='one'",
    )
    .await
    .unwrap();
    let next = zip::claim_root(&b, "one", "worker-b", 60).await.unwrap();
    let tx = a.begin().await.unwrap();
    assert!(
        zip::settle_root_retry(
            &tx,
            "one",
            "{\"attempt\":\"stale\"}",
            RootOutcome::Verified {
                claim: old.clone(),
                node_identity: "node".into(),
                tier: "hot".into(),
                cid: "old-root".into(),
            }
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    assert!(zip::renew_claim(&a, &old, 60).await.is_err());
    let snapshot = zip::snapshot(&b, "one").await.unwrap().unwrap();
    assert_eq!(
        snapshot.batch.terminal_result.as_deref(),
        Some("{\"attempt\":\"initial\"}")
    );
    assert_eq!(snapshot.batch.root_epoch, next.epoch);
    assert_eq!(snapshot.references[0].state, "retained");
    assert_eq!(
        snapshot.references[0].verification_receipt.as_deref(),
        Some("old-receipt")
    );
}

fn admission(id: &str) -> BatchAdmission {
    BatchAdmission {
        id: id.into(),
        owner: "principal".into(),
        source: "direct".into(),
        token: "token".into(),
        fingerprint: "fingerprint".into(),
        bucket: "bucket".into(),
        archive_key: "archive.zip".into(),
        input_identity: "digest".into(),
        captured_options: "{}".into(),
    }
}

#[tokio::test]
async fn batch_owner_survives_archive_absence_and_token_conflict() {
    let db = setup().await;
    let batch = zip::admit(&db, &admission("one")).await.unwrap();
    assert_eq!(batch.id, "one");
    assert_eq!(zip::admit(&db, &admission("two")).await.unwrap().id, "one");
    let mut conflict = admission("three");
    conflict.fingerprint = "changed".into();
    assert!(zip::admit(&db, &conflict).await.is_err());
    assert_eq!(
        zip::read(&db, "one").await.unwrap().unwrap().archive_key,
        "archive.zip"
    );
}

#[tokio::test]
async fn publish_binds_exact_version_and_root_failure_does_not_rollback_objects() {
    let db = setup().await;
    store::bucket::create(&db, "bucket", None).await.unwrap();
    zip::admit(&db, &admission("one")).await.unwrap();
    zip::prepare_manifest(
        &db,
        "one",
        &[
            ManifestItem::Success {
                path: "a.txt".into(),
                object_key: "dest/a.txt".into(),
                cid: "cid-a".into(),
                size: 2,
            },
            ManifestItem::Failure {
                path: "broken".into(),
                code: "invalid_entry".into(),
            },
        ],
    )
    .await
    .unwrap();
    db.execute_unprepared("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('obj','bucket','dest/a.txt','cid-a',2,'cid-a')").await.unwrap();
    db.execute_unprepared("INSERT INTO object_versions (id,bucket,key,kind,object_id,sequence,is_latest,created_at,updated_at,lifecycle_age_started_at) VALUES ('v1','bucket','dest/a.txt','object','obj',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('other','bucket','other','cid-a',2,'cid-a')").await.unwrap();
    db.execute_unprepared("INSERT INTO object_versions (id,bucket,key,kind,object_id,sequence,is_latest,created_at,updated_at,lifecycle_age_started_at) VALUES ('v-other','bucket','other','object','other',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    let tx = db.begin().await.unwrap();
    assert!(
        zip::publish(
            &tx,
            "one",
            &[VersionBinding {
                path: "a.txt".into(),
                version_row_id: "v-other".into()
            }],
            false,
            "{}",
            RootOutcome::Failed {
                code: "path_conflict"
            }
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    let tx = db.begin().await.unwrap();
    let binding = zip::binding_for_published_object(&tx, "a.txt", "obj")
        .await
        .unwrap();
    assert_eq!(binding.version_row_id, "v1");
    zip::publish(
        &tx,
        "one",
        &[binding],
        false,
        "{\"ok\":true}",
        RootOutcome::Failed {
            code: "path_conflict",
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let snapshot = zip::snapshot(&db, "one").await.unwrap().unwrap();
    assert_eq!(snapshot.batch.root_status, "failed");
    assert_eq!(
        snapshot.batch.root_error_code.as_deref(),
        Some("path_conflict")
    );
    assert_eq!(snapshot.entries[0].version_row_id.as_deref(), Some("v1"));
    assert_eq!(
        snapshot.entries[1].error_code.as_deref(),
        Some("invalid_entry")
    );
    assert!(snapshot.references.is_empty());
    assert!(
        zip::publish(
            &db.begin().await.unwrap(),
            "one",
            &[],
            false,
            "{}",
            RootOutcome::Empty
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn invalid_late_binding_cannot_partially_bind_an_earlier_success() {
    let db = setup().await;
    store::bucket::create(&db, "bucket", None).await.unwrap();
    zip::admit(&db, &admission("one")).await.unwrap();
    zip::prepare_manifest(
        &db,
        "one",
        &[
            ManifestItem::Success {
                path: "a".into(),
                object_key: "out/a".into(),
                cid: "cid-a".into(),
                size: 1,
            },
            ManifestItem::Success {
                path: "b".into(),
                object_key: "out/b".into(),
                cid: "cid-b".into(),
                size: 2,
            },
        ],
    )
    .await
    .unwrap();
    db.execute_unprepared("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('obj','bucket','out/a','cid-a',1,'cid-a')").await.unwrap();
    db.execute_unprepared("INSERT INTO object_versions (id,bucket,key,kind,object_id,sequence,is_latest,created_at,updated_at,lifecycle_age_started_at) VALUES ('v1','bucket','out/a','object','obj',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    let tx = db.begin().await.unwrap();
    let result = zip::publish(
        &tx,
        "one",
        &[
            VersionBinding {
                path: "a".into(),
                version_row_id: "v1".into(),
            },
            VersionBinding {
                path: "b".into(),
                version_row_id: "missing".into(),
            },
        ],
        false,
        "{}",
        RootOutcome::Failed {
            code: "path_conflict",
        },
    )
    .await;
    assert!(result.is_err());
    let bound: i64 = tx
        .query_one(sea_orm::Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS count FROM zip_manifest_entries WHERE version_row_id IS NOT NULL",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "count")
        .unwrap();
    assert_eq!(bound, 0);
    tx.rollback().await.unwrap();
    let snapshot = zip::snapshot(&db, "one").await.unwrap().unwrap();
    assert_eq!(snapshot.batch.state, "open");
    assert!(snapshot.entries.iter().all(|e| e.version_row_id.is_none()));
}

#[tokio::test]
async fn unknown_external_commit_and_stale_epoch_are_retained_not_adopted() {
    let db = setup().await;
    zip::admit(&db, &admission("one")).await.unwrap();
    zip::prepare_manifest(
        &db,
        "one",
        &[ManifestItem::Success {
            path: "a".into(),
            object_key: "a".into(),
            cid: "leaf".into(),
            size: 1,
        }],
    )
    .await
    .unwrap();
    let old = zip::claim_root(&db, "one", "worker-1", 60).await.unwrap();
    zip::mark_invoked(&db, &old).await.unwrap();
    zip::mark_unknown(&db, &old).await.unwrap();
    assert_eq!(zip::recovery(&db, "one").await.unwrap().len(), 1);
    let next = zip::claim_root(&db, "one", "worker-2", 60).await.unwrap();
    assert!(next.epoch > old.epoch);
    zip::retain_candidate(&db, &old, "hot-node", "hot", "root-old")
        .await
        .unwrap();
    assert!(
        zip::verify_root(&db, &old, "hot-node", "hot", "root-old", "receipt")
            .await
            .is_err()
    );
    zip::mark_invoked(&db, &next).await.unwrap();
    zip::retain_candidate(&db, &next, "hot-node", "hot", "root-new")
        .await
        .unwrap();
    zip::verify_root(&db, &next, "hot-node", "hot", "root-new", "receipt")
        .await
        .unwrap();
    assert_eq!(
        zip::root_existence(&db, "one", "hot-node", "hot", "root-old")
            .await
            .unwrap(),
        "retained"
    );
    assert_eq!(
        zip::root_existence(&db, "one", "hot-node", "hot", "root-new")
            .await
            .unwrap(),
        "retained"
    );
    assert_eq!(
        zip::root_existence(&db, "one", "other-node", "hot", "root-new")
            .await
            .unwrap(),
        "absent"
    );
}

#[tokio::test]
async fn file_backed_restart_recovers_unknown_intent_and_exact_candidate() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("zip-recovery.db")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let first = store::connect_database(&url).await.unwrap();
    first
        .execute_unprepared("PRAGMA foreign_keys=ON")
        .await
        .unwrap();
    store::run_migrations(&first).await.unwrap();
    zip::admit(&first, &admission("one")).await.unwrap();
    zip::prepare_manifest(
        &first,
        "one",
        &[ManifestItem::Success {
            path: "a".into(),
            object_key: "a".into(),
            cid: "leaf".into(),
            size: 1,
        }],
    )
    .await
    .unwrap();
    let claim = zip::claim_root(&first, "one", "before-restart", 60)
        .await
        .unwrap();
    zip::mark_invoked(&first, &claim).await.unwrap();
    zip::retain_candidate(&first, &claim, "node", "hot", "possibly-committed")
        .await
        .unwrap();
    zip::mark_unknown(&first, &claim).await.unwrap();
    first.close().await.unwrap();

    let reopened = store::connect_database(&url).await.unwrap();
    reopened
        .execute_unprepared("PRAGMA foreign_keys=ON")
        .await
        .unwrap();
    let snapshot = zip::snapshot(&reopened, "one").await.unwrap().unwrap();
    assert_eq!(snapshot.builds[0].status, "unknown");
    assert_eq!(snapshot.references[0].cid, "possibly-committed");
    assert_eq!(snapshot.entries[0].path, "a");
    let new_claim = zip::claim_root(&reopened, "one", "after-restart", 60)
        .await
        .unwrap();
    assert!(new_claim.epoch > claim.epoch);
    assert_eq!(
        zip::root_existence(&reopened, "one", "node", "hot", "possibly-committed")
            .await
            .unwrap(),
        "retained"
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn verified_root_is_adopted_only_by_atomic_publication_and_survives_source_deletion() {
    let db = setup().await;
    store::bucket::create(&db, "bucket", None).await.unwrap();
    zip::admit(&db, &admission("one")).await.unwrap();
    zip::prepare_manifest(
        &db,
        "one",
        &[ManifestItem::Success {
            path: "a".into(),
            object_key: "out/a".into(),
            cid: "leaf".into(),
            size: 1,
        }],
    )
    .await
    .unwrap();
    let claim = zip::claim_root(&db, "one", "worker", 60).await.unwrap();
    zip::mark_invoked(&db, &claim).await.unwrap();
    zip::retain_candidate(&db, &claim, "node", "hot", "root")
        .await
        .unwrap();
    zip::verify_root(
        &db,
        &claim,
        "node",
        "hot",
        "root",
        "recursive-pin-and-dag-verified",
    )
    .await
    .unwrap();
    assert_eq!(
        zip::root_existence(&db, "one", "node", "hot", "root")
            .await
            .unwrap(),
        "retained"
    );
    db.execute_unprepared("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('obj','bucket','out/a','leaf',1,'leaf')").await.unwrap();
    db.execute_unprepared("INSERT INTO object_versions (id,bucket,key,kind,object_id,sequence,is_latest,created_at,updated_at,lifecycle_age_started_at) VALUES ('v1','bucket','out/a','object','obj',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    let binding = [VersionBinding {
        path: "a".into(),
        version_row_id: "v1".into(),
    }];
    let outcome = || RootOutcome::Verified {
        claim: claim.clone(),
        node_identity: "node".into(),
        tier: "hot".into(),
        cid: "root".into(),
    };
    let tx = db.begin().await.unwrap();
    zip::publish(&tx, "one", &binding, false, "{}", outcome())
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        zip::root_existence(&db, "one", "node", "hot", "root")
            .await
            .unwrap(),
        "retained"
    );
    assert_eq!(zip::read(&db, "one").await.unwrap().unwrap().state, "open");
    let tx = db.begin().await.unwrap();
    zip::publish(&tx, "one", &binding, false, "{}", outcome())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        zip::root_existence(&db, "one", "node", "hot", "root")
            .await
            .unwrap(),
        "adopted"
    );
    db.execute_unprepared("DELETE FROM objects WHERE id='obj'")
        .await
        .unwrap();
    db.execute_unprepared("DELETE FROM buckets WHERE name='bucket'")
        .await
        .unwrap();
    let snapshot = zip::snapshot(&db, "one").await.unwrap().unwrap();
    assert_eq!(snapshot.batch.root_status, "complete");
    assert_eq!(snapshot.entries[0].version_row_id.as_deref(), Some("v1"));
    assert_eq!(
        zip::root_existence(&db, "one", "node", "hot", "root")
            .await
            .unwrap(),
        "adopted"
    );
    assert!(
        store::migrations::m20260927_000001_zip_batches::Migration
            .down(&SchemaManager::new(&db))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn old_database_migrates_without_rewriting_existing_objects() {
    use sea_orm_migration::MigratorTrait;
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
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys=ON")
        .await
        .unwrap();
    Previous::up(&db, None).await.unwrap();
    store::bucket::create(&db, "bucket", None).await.unwrap();
    db.execute_unprepared("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('old','bucket','old','legacy',1,'legacy')").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    zip::admit(&db, &admission("new")).await.unwrap();
    let count: i64 = db
        .query_one(sea_orm::Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            "SELECT count(*) AS count FROM objects WHERE id='old' AND cid='legacy'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "count")
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn root_only_retry_reconciles_unknown_without_republishing_manifest_version() {
    let db = setup().await;
    store::bucket::create(&db, "bucket", None).await.unwrap();
    zip::admit(&db, &admission("one")).await.unwrap();
    zip::prepare_manifest(
        &db,
        "one",
        &[
            ManifestItem::Success {
                path: "a".into(),
                object_key: "out/a".into(),
                cid: "leaf".into(),
                size: 1,
            },
            ManifestItem::Failure {
                path: "bad".into(),
                code: "bad_entry".into(),
            },
        ],
    )
    .await
    .unwrap();
    let old = zip::claim_root(&db, "one", "worker-old", 60).await.unwrap();
    zip::mark_invoked(&db, &old).await.unwrap();
    zip::retain_candidate(&db, &old, "node", "hot", "root")
        .await
        .unwrap();
    zip::mark_unknown(&db, &old).await.unwrap();
    db.execute_unprepared("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('obj','bucket','out/a','leaf',1,'leaf')").await.unwrap();
    db.execute_unprepared("INSERT INTO object_versions (id,bucket,key,kind,object_id,sequence,is_latest,created_at,updated_at,lifecycle_age_started_at) VALUES ('v1','bucket','out/a','object','obj',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    let failure_claim = zip::claim_root(&db, "one", "worker-failure", 60)
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    failure_claim
        .publish_failed(
            &tx,
            &[VersionBinding {
                path: "a".into(),
                version_row_id: "v1".into(),
            }],
            false,
            "{\"status\":\"failed\"}",
            "external_unknown",
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
    zip::mark_invoked(&db, &failure_claim).await.unwrap();
    zip::mark_unknown(&db, &failure_claim).await.unwrap();
    let retry = zip::claim_root(&db, "one", "worker-new", 60).await.unwrap();
    assert!(retry.revision > old.revision);
    zip::mark_reconciling(&db, &retry).await.unwrap();
    zip::retain_candidate(&db, &retry, "node", "hot", "root")
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    assert!(
        zip::settle_root_retry(
            &tx,
            "one",
            "{}",
            RootOutcome::Verified {
                claim: retry.clone(),
                node_identity: "node".into(),
                tier: "hot".into(),
                cid: "root".into(),
            }
        )
        .await
        .is_err(),
        "candidate without a verified receipt is not a published root"
    );
    tx.rollback().await.unwrap();
    zip::verify_root(&db, &retry, "node", "hot", "root", "rechecked-dag-and-pin")
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    zip::settle_root_retry(
        &tx,
        "one",
        "{\"status\":\"partial\"}",
        RootOutcome::Verified {
            claim: retry,
            node_identity: "node".into(),
            tier: "hot".into(),
            cid: "root".into(),
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let snapshot = zip::snapshot(&db, "one").await.unwrap().unwrap();
    assert_eq!(snapshot.batch.root_status, "partial");
    assert_eq!(snapshot.entries[0].version_row_id.as_deref(), Some("v1"));
    assert_eq!(snapshot.references.len(), 2);
    assert_eq!(
        snapshot
            .references
            .iter()
            .filter(|r| r.state == "adopted")
            .count(),
        1
    );
    assert_eq!(
        snapshot
            .references
            .iter()
            .filter(|r| r.state == "retained")
            .count(),
        1
    );
    assert!(
        zip::publish(
            &db.begin().await.unwrap(),
            "one",
            &[],
            false,
            "{}",
            RootOutcome::Empty
        )
        .await
        .is_err()
    );
}
