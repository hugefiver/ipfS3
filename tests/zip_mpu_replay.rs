use ipfs_s3_gateway::store::{
    self,
    zip::{self, BatchAdmission, RootOutcome},
};
use s3s::dto::{CompletedPart, ETag};
use sea_orm::{ConnectionTrait, Database, TransactionTrait};
use std::collections::BTreeMap;

fn part(etag: &str) -> CompletedPart {
    CompletedPart {
        part_number: Some(1),
        e_tag: Some(ETag::Strong(etag.into())),
        checksum_crc32: Some("first-crc".into()),
        ..Default::default()
    }
}

async fn setup() -> sea_orm::DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "bucket", None).await.unwrap();
    zip::admit(
        &db,
        &BatchAdmission {
            id: "upload".into(),
            owner: "alice".into(),
            source: "mpu".into(),
            token: "upload".into(),
            fingerprint: "mpu:upload:bucket:a.zip:out/:true".into(),
            bucket: "bucket".into(),
            archive_key: "a.zip".into(),
            input_identity: "upload".into(),
            captured_options: r#"{"target_prefix":"out/","root_enabled":true}"#.into(),
        },
    )
    .await
    .unwrap();
    db
}

#[tokio::test]
async fn complete_contract_is_exact_and_terminal_is_immutable_after_archive_deletion() {
    let db = setup().await;
    let original = [part("part-cid")];
    assert!(
        BatchAdmission::replay_lookup(&db, "alice", "bucket", "a.zip", "upload", &original)
            .await
            .unwrap()
            .is_pending()
    );
    BatchAdmission::capture_mpu_complete(&db, "alice", "bucket", "a.zip", "upload", &original)
        .await
        .unwrap();
    assert!(
        BatchAdmission::capture_mpu_complete(&db, "bob", "bucket", "a.zip", "upload", &original)
            .await
            .is_err()
    );
    let changed_etag = [part("different")];
    let mut changed_crc = part("part-cid");
    changed_crc.checksum_crc32 = Some("changed-crc".into());
    for parts in [&changed_etag[..], &[changed_crc][..]] {
        assert!(
            BatchAdmission::capture_mpu_complete(&db, "alice", "bucket", "a.zip", "upload", parts)
                .await
                .is_err()
        );
        assert!(
            BatchAdmission::replay_lookup(&db, "alice", "bucket", "a.zip", "upload", parts)
                .await
                .is_err()
        );
    }
    for (owner, bucket, key) in [
        ("bob", "bucket", "a.zip"),
        ("alice", "other", "a.zip"),
        ("alice", "bucket", "else.zip"),
    ] {
        assert!(
            BatchAdmission::replay_lookup(&db, owner, bucket, key, "upload", &original)
                .await
                .is_err()
        );
    }
    zip::prepare_manifest(&db, "upload", &[]).await.unwrap();
    BatchAdmission::capture_prepared_archive(
        &db,
        "alice",
        "bucket",
        "a.zip",
        "upload",
        &original,
        "archive-cid",
    )
    .await
    .unwrap();
    assert!(
        zip::read(&db, "upload")
            .await
            .unwrap()
            .unwrap()
            .terminal_result
            .is_none()
    );
    assert!(
        BatchAdmission::capture_prepared_archive(
            &db,
            "alice",
            "bucket",
            "a.zip",
            "upload",
            &original,
            "different-archive"
        )
        .await
        .is_err()
    );
    assert!(
        BatchAdmission::capture_prepared_archive(
            &db,
            "bob",
            "bucket",
            "a.zip",
            "upload",
            &original,
            "archive-cid",
        )
        .await
        .is_err()
    );
    assert!(
        BatchAdmission::capture_prepared_archive(
            &db,
            "alice",
            "bucket",
            "a.zip",
            "upload",
            &changed_etag,
            "archive-cid",
        )
        .await
        .is_err()
    );
    assert!(db.execute_unprepared(
        r#"UPDATE zip_batches SET terminal_result='{"archive_cid":"archive-cid"}' WHERE id='upload'"#,
    ).await.is_err());
    db.execute_unprepared("INSERT INTO objects (id,bucket,key,cid,size,etag,multipart) VALUES ('published','bucket','a.zip','archive-cid',42,'archive-cid',TRUE)").await.unwrap();
    db.execute_unprepared("INSERT INTO object_versions (id,bucket,key,kind,object_id,sequence,is_latest,created_at,updated_at,lifecycle_age_started_at) VALUES ('v1','bucket','a.zip','object','published',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    let tx = db.begin().await.unwrap();
    zip::publish(
        &tx,
        "upload",
        &[],
        true,
        r#"{"archive_cid":"archive-cid"}"#,
        RootOutcome::Disabled,
    )
    .await
    .unwrap();
    let xml = ipfs_s3_gateway::zip::response::complete_multipart_result_xml(
        "bucket",
        "a.zip",
        "archive-cid",
    );
    assert!(
        BatchAdmission::completed_upload_result(
            &tx,
            "alice",
            "bucket",
            "a.zip",
            "upload",
            &original,
            "published",
            "wrong-cid",
            42,
            None,
            None,
            &xml,
            BTreeMap::from([("etag".into(), "\"wrong-cid\"".into())]),
        )
        .await
        .is_err()
    );
    BatchAdmission::completed_upload_result(
        &tx,
        "alice",
        "bucket",
        "a.zip",
        "upload",
        &original,
        "published",
        "archive-cid",
        42,
        None,
        None,
        &xml,
        BTreeMap::from([("etag".into(), "\"archive-cid\"".into())]),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    db.execute_unprepared("DELETE FROM object_versions WHERE id='v1'")
        .await
        .unwrap();
    db.execute_unprepared("DELETE FROM objects WHERE id='published'")
        .await
        .unwrap();
    let lookup =
        BatchAdmission::replay_lookup(&db, "alice", "bucket", "a.zip", "upload", &original)
            .await
            .unwrap()
            .completed()
            .unwrap();
    assert_eq!(lookup.archive_cid, "archive-cid");
    assert_eq!(lookup.archive_size, 42);
    assert_eq!(lookup.public_version_id, None);
    assert_eq!(lookup.response_xml, xml);
    assert!(
        BatchAdmission::replay_lookup(&db, "bob", "bucket", "a.zip", "upload", &original)
            .await
            .is_err()
    );
    let tx = db.begin().await.unwrap();
    assert!(
        BatchAdmission::completed_upload_result(
            &tx,
            "alice",
            "bucket",
            "a.zip",
            "upload",
            &original,
            "published",
            "archive-cid",
            43,
            None,
            None,
            &xml,
            BTreeMap::from([("etag".into(), "\"archive-cid\"".into())]),
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn database_rejects_partial_result_shape() {
    let db = setup().await;
    let original = [part("etag")];
    BatchAdmission::capture_mpu_complete(&db, "alice", "bucket", "a.zip", "upload", &original)
        .await
        .unwrap();
    BatchAdmission::capture_prepared_archive(
        &db, "alice", "bucket", "a.zip", "upload", &original, "cid",
    )
    .await
    .unwrap();
    assert!(
        db.execute_unprepared(
            "UPDATE zip_mpu_replays SET archive_cid='cid' WHERE batch_id='upload'"
        )
        .await
        .is_err()
    );
    assert!(db.execute_unprepared("UPDATE zip_mpu_replays SET archive_cid='cid',archive_size=-1,response_xml='<x/>' WHERE batch_id='upload'")
        .await.is_err());
    assert!(db.execute_unprepared("UPDATE zip_mpu_replays SET archive_cid='cid',archive_size=1,response_xml='<x/>',server_side_encryption='not-AES256' WHERE batch_id='upload'")
        .await.is_err());
    assert!(
        db.execute_unprepared(
            "UPDATE zip_mpu_replays SET prepared_archive_cid='changed' WHERE batch_id='upload'"
        )
        .await
        .is_ok()
    );
    assert!(db.execute_unprepared("UPDATE zip_mpu_replays SET archive_cid='cid',archive_size=1,response_xml='<x/>',response_headers_json='{}' WHERE batch_id='upload'").await.is_err());
}

#[tokio::test]
async fn contract_preserves_order_etag_presence_and_all_checksum_fields() {
    let db = setup().await;
    let mut first = part("etag");
    first.checksum_crc32c = Some("crc32c".into());
    first.checksum_crc64nvme = Some("crc64".into());
    first.checksum_sha1 = Some("sha1".into());
    first.checksum_sha256 = Some("sha256".into());
    let mut second = part("second");
    second.part_number = Some(3);
    second.e_tag = None;
    let original = [first.clone(), second.clone()];
    BatchAdmission::capture_mpu_complete(&db, "alice", "bucket", "a.zip", "upload", &original)
        .await
        .unwrap();
    let stored = db
        .query_one(sea_orm::Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            "SELECT request_contract FROM zip_mpu_replays WHERE batch_id='upload'",
        ))
        .await
        .unwrap()
        .unwrap();
    let contract: String = stored.try_get("", "request_contract").unwrap();
    let parts: serde_json::Value = serde_json::from_str(&contract).unwrap();
    assert_eq!(parts[0]["part_number"], 1);
    assert_eq!(parts[1]["part_number"], 3);
    assert!(parts[1]["etag"].is_null());
    for mutate in ["crc32", "crc32c", "crc64nvme", "sha1", "sha256"] {
        let mut changed = original.clone();
        match mutate {
            "crc32" => changed[0].checksum_crc32 = None,
            "crc32c" => changed[0].checksum_crc32c = None,
            "crc64nvme" => changed[0].checksum_crc64nvme = None,
            "sha1" => changed[0].checksum_sha1 = None,
            "sha256" => changed[0].checksum_sha256 = None,
            _ => unreachable!(),
        }
        assert!(
            BatchAdmission::replay_lookup(&db, "alice", "bucket", "a.zip", "upload", &changed)
                .await
                .is_err(),
            "{mutate}"
        );
    }
    let mut changed = original.clone();
    changed[1].e_tag = Some(ETag::Strong("second".into()));
    assert!(
        BatchAdmission::replay_lookup(&db, "alice", "bucket", "a.zip", "upload", &changed)
            .await
            .is_err()
    );
    changed = original.clone();
    changed[0].e_tag = Some(ETag::Weak("etag".into()));
    assert!(
        BatchAdmission::replay_lookup(&db, "alice", "bucket", "a.zip", "upload", &changed)
            .await
            .is_err()
    );
    changed = original.clone();
    changed[1].part_number = Some(2);
    assert!(
        BatchAdmission::replay_lookup(&db, "alice", "bucket", "a.zip", "upload", &changed)
            .await
            .is_err()
    );
}
