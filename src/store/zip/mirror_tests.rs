//! Atomic ZIP manifest mirroring on a file-backed SQLite database with two connections.
use std::collections::BTreeMap;

use super::{batch, execution, manifest};
use crate::store;
use crate::store as gateway_store;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, TransactionTrait};
use sea_orm_migration::{MigrationTrait, SchemaManager};
use sha2::{Digest, Sha256};

async fn setup() -> (tempfile::TempDir, DatabaseConnection, DatabaseConnection) {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("mirror.db")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let a = gateway_store::connect_database(&url).await.unwrap();
    gateway_store::run_migrations(&a).await.unwrap();
    let manager = SchemaManager::new(&a);
    if a.query_one(sea_orm::Statement::from_string(
        DatabaseBackend::Sqlite,
        "SELECT name FROM sqlite_master WHERE name='zip_batches'",
    ))
    .await
    .unwrap()
    .is_none()
    {
        gateway_store::migrations::m20260927_000001_zip_batches::Migration
            .up(&manager)
            .await
            .unwrap();
    }
    if a.query_one(sea_orm::Statement::from_string(
        DatabaseBackend::Sqlite,
        "SELECT name FROM sqlite_master WHERE name='zip_v2_executions'",
    ))
    .await
    .unwrap()
    .is_none()
    {
        gateway_store::migrations::m20260927_000004_zip_v2_execution::Migration
            .up(&manager)
            .await
            .unwrap();
    }
    gateway_store::bucket::create(&a, "bucket", None)
        .await
        .unwrap();
    let b = gateway_store::connect_database(&url).await.unwrap();
    (directory, a, b)
}

fn request() -> execution::Admission {
    let request_contract = "{\"canonical\":true}".to_owned();
    execution::Admission {
        id: "batch".into(),
        owner: "owner".into(),
        source: "direct".into(),
        token: "signed-token".into(),
        request_fingerprint: hex::encode(Sha256::digest(request_contract.as_bytes())),
        request_contract,
        bucket: "bucket".into(),
        source_key: "source.zip".into(),
        captured_options: "{}".into(),
    }
}

fn mirror() -> batch::BatchAdmission {
    let request = request();
    batch::BatchAdmission {
        id: request.id,
        owner: request.owner,
        source: request.source,
        token: request.token,
        // Neither value attests the ZIP bytes. Only execution::bind_clean_input
        // persists the actual digest after clean EOF.
        fingerprint: "pending".into(),
        input_identity: "pending".into(),
        bucket: request.bucket,
        archive_key: request.source_key,
        captured_options: request.captured_options,
    }
}

fn items(cid: &str) -> Vec<execution::ManifestItem> {
    vec![
        execution::ManifestItem::Success {
            path: "out/file".into(),
            object_key: "out/file".into(),
            cid: cid.into(),
            size: 3,
        },
        execution::ManifestItem::Failure {
            path: "failed/file".into(),
            code: "invalid_zip".into(),
        },
    ]
}

fn legacy_items(cid: &str) -> Vec<manifest::ManifestItem> {
    vec![
        manifest::ManifestItem::Success {
            path: "out/file".into(),
            object_key: "out/file".into(),
            cid: cid.into(),
            size: 3,
        },
        manifest::ManifestItem::Failure {
            path: "failed/file".into(),
            code: "invalid_zip".into(),
        },
    ]
}

async fn claimed(db: &DatabaseConnection) -> execution::Claim {
    execution::admit(db, &request()).await.unwrap();
    let claim = execution::claim(db, "batch", "worker", 30)
        .await
        .unwrap()
        .unwrap();
    execution::bind_clean_input(db, &claim, &"a".repeat(64), "input-art", 10)
        .await
        .unwrap();
    claim
}

fn ids() -> BTreeMap<String, String> {
    BTreeMap::from([("out/file".into(), "mutation-output".into())])
}

async fn count(db: &DatabaseConnection, table: &str) -> i64 {
    assert!(
        [
            "zip_v2_targets",
            "zip_v2_manifest",
            "zip_batches",
            "zip_manifest_entries"
        ]
        .contains(&table)
    );
    db.query_one(sea_orm::Statement::from_string(
        DatabaseBackend::Sqlite,
        format!("SELECT count(*) AS n FROM {table}"),
    ))
    .await
    .unwrap()
    .unwrap()
    .try_get("", "n")
    .unwrap()
}

#[tokio::test]
async fn later_mirror_failure_rolls_back_guards_and_both_manifests_then_replays_exactly() {
    let (_directory, a, b) = setup().await;
    let claim = claimed(&a).await;

    let tx = a.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    execution::admit_manifest_in_transaction(&tx, &claim, &items("cid-first"), &ids())
        .await
        .unwrap();
    batch::BatchAdmission::admit_in_transaction(&tx, &mirror())
        .await
        .unwrap();
    let mut invalid_items = legacy_items("cid-first");
    invalid_items.push(invalid_items[0].clone());
    assert!(
        manifest::ManifestItem::prepare_manifest_in_transaction(&tx, "batch", &invalid_items)
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    for table in [
        "zip_batches",
        "zip_manifest_entries",
        "zip_v2_manifest",
        "zip_v2_targets",
    ] {
        assert_eq!(count(&b, table).await, 0, "{table} survived rollback");
    }
    assert_eq!(
        execution::read(&b, "batch").await.unwrap().unwrap().state,
        "pending"
    );

    let tx = a.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    execution::admit_manifest_in_transaction(&tx, &claim, &items("cid-first"), &ids())
        .await
        .unwrap();
    let row = batch::BatchAdmission::admit_in_transaction(&tx, &mirror())
        .await
        .unwrap();
    assert_eq!(
        (&row.fingerprint[..], &row.input_identity[..]),
        ("pending", "pending")
    );
    manifest::ManifestItem::prepare_manifest_in_transaction(
        &tx,
        "batch",
        &legacy_items("cid-first"),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    for (table, expected) in [
        ("zip_batches", 1),
        ("zip_manifest_entries", 2),
        ("zip_v2_manifest", 2),
        ("zip_v2_targets", 1),
    ] {
        assert_eq!(count(&b, table).await, expected, "{table}");
    }
    let snapshot = gateway_store::zip::snapshot(&b, "batch")
        .await
        .unwrap()
        .unwrap();
    assert!(snapshot.batch.manifest_prepared);
    assert_eq!(
        snapshot
            .entries
            .iter()
            .find(|row| row.path == "out/file")
            .unwrap()
            .cid
            .as_deref(),
        Some("cid-first")
    );

    let tx = b.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    batch::BatchAdmission::admit_in_transaction(&tx, &mirror())
        .await
        .unwrap();
    let mut reordered = legacy_items("cid-first");
    reordered.reverse();
    manifest::ManifestItem::prepare_manifest_in_transaction(&tx, "batch", &reordered)
        .await
        .unwrap();
    assert!(
        manifest::ManifestItem::prepare_manifest_in_transaction(
            &tx,
            "batch",
            &legacy_items("cid-other")
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    assert_eq!(count(&a, "zip_manifest_entries").await, 2);
    assert_eq!(
        gateway_store::zip::snapshot(&a, "batch")
            .await
            .unwrap()
            .unwrap()
            .entries
            .iter()
            .find(|row| row.path == "out/file")
            .unwrap()
            .cid
            .as_deref(),
        Some("cid-first")
    );

    let tx = b.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    assert!(
        execution::admit_manifest_in_transaction(&tx, &claim, &items("cid-other"), &ids())
            .await
            .is_err()
    );
    let mut false_digest = mirror();
    false_digest.input_identity = "a".repeat(64);
    assert!(
        batch::BatchAdmission::admit_in_transaction(&tx, &false_digest)
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    assert_eq!(count(&a, "zip_v2_targets").await, 1);
}

#[tokio::test]
async fn terminal_legacy_source_cannot_be_borrowed_and_old_prepare_stays_strict() {
    let (_directory, a, b) = setup().await;
    let legacy = mirror();
    batch::admit(&a, &legacy).await.unwrap();
    manifest::prepare_manifest(&a, "batch", &legacy_items("cid-first"))
        .await
        .unwrap();
    assert!(
        manifest::prepare_manifest(&a, "batch", &legacy_items("cid-first"))
            .await
            .is_err()
    );
    a.execute_unprepared("UPDATE zip_batches SET state='published',source_published=TRUE,terminal_result='{}' WHERE id='batch'")
        .await.unwrap();
    // Legacy wrapper continues returning the old snapshot, but v2 may not
    // borrow the published source=true batch as its root/status authority.
    assert!(batch::admit(&b, &legacy).await.unwrap().source_published);
    let claim = claimed(&b).await;
    let tx = b.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    execution::admit_manifest_in_transaction(&tx, &claim, &items("cid-v2"), &ids())
        .await
        .unwrap();
    assert!(
        batch::BatchAdmission::admit_in_transaction(&tx, &legacy)
            .await
            .is_err()
    );
    assert!(
        manifest::ManifestItem::prepare_manifest_in_transaction(
            &tx,
            "batch",
            &legacy_items("cid-first")
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        execution::read(&a, "batch").await.unwrap().unwrap().state,
        "pending"
    );
    assert_eq!(count(&a, "zip_v2_targets").await, 0);
    assert_eq!(count(&a, "zip_v2_manifest").await, 0);
    assert_eq!(count(&a, "zip_manifest_entries").await, 2);
}
