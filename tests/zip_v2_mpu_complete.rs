//! Stage4: real SigV4 Complete publishes only ZIP v2 outputs and freezes replay.
#[allow(dead_code)]
mod support;

use futures_util::FutureExt as _;
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{store, store::object_version::BucketVersioningState};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use sha2::{Digest, Sha256};
use support::decompress::{
    AddReply, KuboScript, TestHarness, legal_single_entry_zip, start_harness_with_root_default,
};

fn create_headers(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "false"),
        ("x-ipfs3-zip-publish-extracted", "true"),
        ("x-ipfs3-zip-targets", "none"),
        ("x-ipfs3-zip-token", token),
    ] {
        headers.insert(name, HeaderValue::from_str(value).unwrap());
    }
    headers
}

async fn setup_with_targets(
    root_enabled: bool,
    source_exists: bool,
    targets: &str,
) -> (TestHarness, String, Vec<u8>) {
    let archive = legal_single_entry_zip();
    let harness = start_harness_with_root_default(
        KuboScript {
            add_replies: (if source_exists {
                vec![AddReply::Ok("QmExistingSource")]
            } else {
                vec![]
            })
            .into_iter()
            .chain([
                AddReply::Ok("QmArchive"),
                AddReply::Ok("QmOutput"),
                AddReply::Ok("QmReplacement"),
            ])
            .collect(),
            cat_bodies: [
                ("QmPart".to_owned(), archive.clone()),
                ("QmArchive".to_owned(), archive.clone()),
            ]
            .into(),
        },
        root_enabled,
    )
    .await;
    if source_exists {
        let put = support::sigv4::send_sigv4(
            reqwest::Method::PUT,
            &harness.endpoint,
            &harness.bucket,
            "archive.zip",
            &[],
            b"existing source".to_vec(),
            HeaderMap::new(),
            "test",
        )
        .await;
        assert_eq!(put.status(), StatusCode::OK);
    }
    let mut controls = create_headers("stage4-token");
    controls.insert(
        "x-ipfs3-zip-targets",
        HeaderValue::from_str(targets).unwrap(),
    );
    let create = support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploads", ""), ("decompress-zip", "out/")],
        vec![],
        controls,
        "test",
    )
    .await;
    let status = create.status();
    let body = create.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    let id = body
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_owned();
    store::multipart::upsert_part(
        harness.state.store.db(),
        &id,
        1,
        "QmPart",
        archive.len() as i64,
        "QmPart",
    )
    .await
    .unwrap();
    (harness, id, archive)
}

async fn setup_with(root_enabled: bool, source_exists: bool) -> (TestHarness, String, Vec<u8>) {
    setup_with_targets(root_enabled, source_exists, "none").await
}

async fn setup() -> (TestHarness, String, Vec<u8>) {
    setup_with(false, false).await
}

async fn status_for(harness: &TestHarness, id: &str) -> reqwest::Response {
    support::sigv4::send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("ipfs3-zip-batch", id)],
        vec![],
        HeaderMap::new(),
        "test",
    )
    .await
}

async fn assert_settled_zero_success(harness: &TestHarness, id: &str, status: &str) {
    let first = complete(harness, id, None).await;
    assert_eq!(first.status(), StatusCode::BAD_REQUEST);
    let headers = first.headers().clone();
    let body = first.text().await.unwrap();
    assert!(body.contains("<Code>InvalidRequest</Code>"), "{body}");
    assert_eq!(headers["x-ipfs-s3-zip-batch-id"], id);
    assert_eq!(headers["x-ipfs-s3-zip-batch-status"], status);
    assert!(!headers.contains_key(http::header::ETAG));
    assert_eq!(count(harness, "objects").await, 0);
    assert_eq!(count(harness, "multipart_uploads").await, 0);
    let terminal = store::zip::execution::read(harness.state.store.db(), id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.state, "completed");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&terminal.terminal_result.unwrap()).unwrap()["status"],
        status
    );
    let batch = status_for(harness, id).await;
    assert_eq!(batch.status(), StatusCode::OK);
    let batch_xml = batch.text().await.unwrap();
    assert!(
        batch_xml.contains(&format!("<BatchId>{id}</BatchId>")),
        "{batch_xml}"
    );
    assert!(
        batch_xml.contains(&format!("<State>{status}</State>")),
        "{batch_xml}"
    );
    assert!(
        batch_xml.contains(&format!("<BatchStatus>{status}</BatchStatus>")),
        "{batch_xml}"
    );
    assert!(
        batch_xml.contains("<SourcePublished>false</SourcePublished>"),
        "{batch_xml}"
    );
    assert!(
        batch_xml.contains("<RootStatus>empty</RootStatus>"),
        "{batch_xml}"
    );
    assert!(!batch_xml.contains("<RootCID>"));
    let wrong_key = support::sigv4::send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "not-the-archive.zip",
        &[("ipfs3-zip-batch", id)],
        vec![],
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(wrong_key.status(), StatusCode::NOT_FOUND);
    let additions = harness.captured_add_file_bytes().len();
    let cats = harness
        .kubo
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|request| request.url.path() == "/api/v0/cat")
        .count();
    let replay = complete(harness, id, None).await;
    assert_eq!(replay.status(), StatusCode::BAD_REQUEST);
    assert_eq!(replay.headers(), &headers);
    assert_eq!(replay.text().await.unwrap(), body);
    let repeated_status = status_for(harness, id).await;
    assert_eq!(repeated_status.status(), StatusCode::OK);
    assert_eq!(repeated_status.text().await.unwrap(), batch_xml);
    assert_eq!(harness.captured_add_file_bytes().len(), additions);
    assert_eq!(
        harness
            .kubo
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .filter(|request| request.url.path() == "/api/v0/cat")
            .count(),
        cats
    );
    assert_eq!(count(harness, "objects").await, 0);

    // Older settled receipts had no Date header. The upgraded route derives
    // the original publication date instead of introducing a replay timestamp.
    let db = harness.state.store.db();
    let saved = db
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT response_headers FROM zip_v2_mpu_completions WHERE upload_id=?",
            vec![id.into()],
        ))
        .await
        .unwrap()
        .unwrap();
    let mut stored: serde_json::Value =
        serde_json::from_str(&saved.try_get::<String>("", "response_headers").unwrap()).unwrap();
    stored.as_object_mut().unwrap().remove("date");
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "UPDATE zip_v2_mpu_completions SET response_headers=? WHERE upload_id=?",
        vec![stored.to_string().into(), id.into()],
    ))
    .await
    .unwrap();
    let upgraded_replay = complete(harness, id, None).await;
    assert_eq!(upgraded_replay.status(), StatusCode::BAD_REQUEST);
    assert_eq!(upgraded_replay.headers(), &headers);
    assert_eq!(upgraded_replay.text().await.unwrap(), body);
    assert_eq!(harness.captured_add_file_bytes().len(), additions);
}

#[tokio::test]
async fn no_successful_entries_commit_an_explicit_empty_root_without_source_object() {
    let archive = legal_single_entry_zip();
    let harness = start_harness_with_root_default(
        KuboScript {
            add_replies: vec![
                AddReply::Ok("QmArchive"),
                AddReply::Error(StatusCode::INTERNAL_SERVER_ERROR, "entry add refused"),
            ],
            cat_bodies: [
                ("QmPart".to_owned(), archive.clone()),
                ("QmArchive".to_owned(), archive.clone()),
            ]
            .into(),
        },
        true,
    )
    .await;
    let create = support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploads", ""), ("decompress-zip", "out/")],
        vec![],
        create_headers("empty-entries"),
        "test",
    )
    .await;
    assert_eq!(create.status(), StatusCode::OK);
    let body = create.text().await.unwrap();
    let id = body
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap();
    store::multipart::upsert_part(
        harness.state.store.db(),
        id,
        1,
        "QmPart",
        archive.len() as i64,
        "QmPart",
    )
    .await
    .unwrap();

    assert_settled_zero_success(&harness, id, "failed").await;
}

#[tokio::test]
async fn empty_zip_settles_without_publishing_a_fictitious_source_or_root() {
    let mut archive = vec![0_u8; 22];
    archive[..4].copy_from_slice(b"PK\x05\x06");
    let harness = start_harness_with_root_default(
        KuboScript {
            add_replies: vec![AddReply::Ok("QmEmptyArchive")],
            cat_bodies: [
                ("QmPart".to_owned(), archive.clone()),
                ("QmEmptyArchive".to_owned(), archive.clone()),
            ]
            .into(),
        },
        true,
    )
    .await;
    let create = support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploads", ""), ("decompress-zip", "out/")],
        vec![],
        create_headers("actually-empty-zip"),
        "test",
    )
    .await;
    assert_eq!(create.status(), StatusCode::OK);
    let body = create.text().await.unwrap();
    let id = body
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap();
    store::multipart::upsert_part(
        harness.state.store.db(),
        id,
        1,
        "QmPart",
        archive.len() as i64,
        "QmPart",
    )
    .await
    .unwrap();
    assert_settled_zero_success(&harness, id, "empty").await;
}

#[tokio::test]
async fn malformed_empty_eocd_cannot_masquerade_as_a_settled_empty_batch() {
    for corruption in ["truncated", "trailer", "nonzero-count"] {
        let mut archive = vec![0_u8; 22];
        archive[..4].copy_from_slice(b"PK\x05\x06");
        match corruption {
            "truncated" => {
                archive.pop();
            }
            "trailer" => archive.push(b'!'),
            "nonzero-count" => archive[10] = 1,
            _ => unreachable!(),
        }
        let harness = start_harness_with_root_default(
            KuboScript {
                add_replies: vec![AddReply::Ok("QmInvalidEmpty")],
                cat_bodies: [
                    ("QmPart".to_owned(), archive.clone()),
                    ("QmInvalidEmpty".to_owned(), archive.clone()),
                ]
                .into(),
            },
            true,
        )
        .await;
        let created = support::sigv4::send_sigv4(
            reqwest::Method::POST,
            &harness.endpoint,
            &harness.bucket,
            "archive.zip",
            &[("uploads", ""), ("decompress-zip", "out/")],
            vec![],
            create_headers(&format!("invalid-empty-{corruption}")),
            "test",
        )
        .await;
        assert_eq!(created.status(), StatusCode::OK);
        let body = created.text().await.unwrap();
        let id = body
            .split("<UploadId>")
            .nth(1)
            .unwrap()
            .split("</UploadId>")
            .next()
            .unwrap();
        store::multipart::upsert_part(
            harness.state.store.db(),
            id,
            1,
            "QmPart",
            archive.len() as i64,
            "QmPart",
        )
        .await
        .unwrap();
        let complete = complete(&harness, id, None).await;
        assert_ne!(complete.status(), StatusCode::OK, "{corruption}");
        assert_eq!(count(&harness, "multipart_uploads").await, 1);
        assert_eq!(count(&harness, "objects").await, 0);
        assert_eq!(count(&harness, "zip_batches").await, 0);
        assert_ne!(status_for(&harness, id).await.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn extracted_pin_target_replans_with_exact_published_output_version() {
    let (harness, id, _) = setup_with_targets(false, false, "extracted").await;
    let result = complete(&harness, &id, None).await;
    let status = result.status();
    let xml = result.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{xml}");
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "out/file.txt")
            .await
            .unwrap()
            .cid,
        "QmOutput"
    );
    assert_eq!(count(&harness, "multipart_uploads").await, 0);
}

async fn complete(harness: &TestHarness, id: &str, checksum: Option<&str>) -> reqwest::Response {
    let checksum = checksum
        .map(|sum| format!("<ChecksumSHA256>{sum}</ChecksumSHA256>"))
        .unwrap_or_default();
    let body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"QmPart\"</ETag>{checksum}</Part></CompleteMultipartUpload>"
    );
    support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploadId", id)],
        body.into_bytes(),
        HeaderMap::new(),
        "test",
    )
    .await
}

async fn count(harness: &TestHarness, table: &str) -> i64 {
    harness
        .state
        .store
        .db()
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            format!("SELECT COUNT(*) AS n FROM {table}"),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

#[tokio::test]
async fn complete_replay_after_output_overwrite_or_delete_for_every_versioning_state() {
    use base64::Engine as _;
    for versioning in [
        BucketVersioningState::Unversioned,
        BucketVersioningState::Enabled,
        BucketVersioningState::Suspended,
    ] {
        for source_exists in [false, true] {
            let (harness, id, archive) = setup_with(false, source_exists).await;
            if versioning != BucketVersioningState::Unversioned {
                store::bucket::set_versioning_state(
                    harness.state.store.db(),
                    &harness.bucket,
                    versioning,
                )
                .await
                .unwrap();
            }
            let checksum =
                base64::engine::general_purpose::STANDARD.encode(Sha256::digest(&archive));
            let response = complete(&harness, &id, Some(&checksum)).await;
            let status = response.status();
            let headers = response.headers().clone();
            let xml = response.text().await.unwrap();
            assert_eq!(status, StatusCode::OK, "{xml}");
            assert!(
                xml.contains("<SourcePublished>false</SourcePublished>"),
                "{xml}"
            );
            assert_eq!(headers["x-ipfs-s3-zip-batch-id"], id);
            assert!(!headers.contains_key(http::header::ETAG));
            assert!(!headers.contains_key("x-amz-version-id"));
            assert_eq!(count(&harness, "multipart_uploads").await, 0);
            assert_eq!(count(&harness, "multipart_parts").await, 0);
            assert_eq!(count(&harness, "zip_v2_mpu_completions").await, 1);
            assert_eq!(
                store::object::get_latest(
                    harness.state.store.db(),
                    &harness.bucket,
                    "out/file.txt"
                )
                .await
                .unwrap()
                .cid,
                "QmOutput"
            );
            let existing =
                store::object::get_latest(harness.state.store.db(), &harness.bucket, "archive.zip")
                    .await;
            if source_exists {
                assert_eq!(existing.unwrap().cid, "QmExistingSource");
            } else {
                assert!(existing.is_err());
            }
            let written = harness.captured_add_file_bytes();
            let initial_count = 2 + usize::from(source_exists);
            assert_eq!(written.len(), initial_count);
            assert_eq!(written[initial_count - 2], archive);
            let overwrite = support::sigv4::send_sigv4(
                reqwest::Method::PUT,
                &harness.endpoint,
                &harness.bucket,
                "out/file.txt",
                &[],
                b"replacement".to_vec(),
                HeaderMap::new(),
                "test",
            )
            .await;
            assert_eq!(overwrite.status(), StatusCode::OK);
            let replay = complete(&harness, &id, Some(&checksum)).await;
            assert_eq!(replay.status(), StatusCode::OK);
            assert_eq!(
                replay.headers()["x-ipfs-s3-zip-root-status"],
                headers["x-ipfs-s3-zip-root-status"]
            );
            assert_eq!(replay.text().await.unwrap(), xml);
            assert_eq!(harness.captured_add_file_bytes().len(), initial_count + 1);
            let changed = complete(&harness, &id, None).await;
            assert_eq!(changed.status(), StatusCode::CONFLICT);
            assert_eq!(harness.captured_add_file_bytes().len(), initial_count + 1);
            let mut other_token = HeaderMap::new();
            other_token.insert(
                "x-ipfs3-zip-token",
                HeaderValue::from_static("different-token"),
            );
            let token_body = format!(
                "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"QmPart\"</ETag><ChecksumSHA256>{checksum}</ChecksumSHA256></Part></CompleteMultipartUpload>"
            );
            let token_conflict = support::sigv4::send_sigv4(
                reqwest::Method::POST,
                &harness.endpoint,
                &harness.bucket,
                "archive.zip",
                &[("uploadId", &id)],
                token_body.into_bytes(),
                other_token,
                "test",
            )
            .await;
            assert_eq!(token_conflict.status(), StatusCode::CONFLICT);
            let delete = support::sigv4::send_sigv4(
                reqwest::Method::DELETE,
                &harness.endpoint,
                &harness.bucket,
                "out/file.txt",
                &[],
                vec![],
                HeaderMap::new(),
                "test",
            )
            .await;
            assert_eq!(delete.status(), StatusCode::NO_CONTENT);
            let replay = complete(&harness, &id, Some(&checksum)).await;
            assert_eq!(replay.status(), StatusCode::OK);
            assert_eq!(replay.text().await.unwrap(), xml);
            assert_eq!(harness.captured_add_file_bytes().len(), initial_count + 1);
        }
    }
}

#[tokio::test]
async fn failed_root_is_warning_and_exact_outputs_still_commit() {
    let (harness, id, _) = setup_with(true, false).await;
    let response = complete(&harness, &id, None).await;
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers["x-ipfs-s3-zip-root-status"], "failed");
    assert!(!headers.contains_key("x-ipfs-s3-zip-root-cid"));
    assert!(!headers.contains_key(http::header::ETAG));
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "out/file.txt")
            .await
            .unwrap()
            .cid,
        "QmOutput"
    );
    let replay = complete(&harness, &id, None).await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.text().await.unwrap(), body);
    assert_eq!(harness.captured_add_file_bytes().len(), 2);
}

#[tokio::test]
async fn rollback_keeps_upload_and_retry_uses_retained_manifest_without_new_kubo_io() {
    let (harness, id, _) = setup().await;
    let db = harness.state.store.db();
    db.execute_unprepared("CREATE TRIGGER block_v2_complete BEFORE DELETE ON multipart_uploads BEGIN SELECT RAISE(ABORT, 'forced publication rollback'); END").await.unwrap();
    let first = complete(&harness, &id, None).await;
    assert_ne!(first.status(), StatusCode::OK);
    assert_eq!(count(&harness, "multipart_uploads").await, 1);
    assert_eq!(count(&harness, "objects").await, 0);
    assert_eq!(count(&harness, "object_versions").await, 0);
    assert_eq!(count(&harness, "zip_v2_targets").await, 1);
    assert_eq!(harness.captured_add_file_bytes().len(), 2);
    db.execute_unprepared("DROP TRIGGER block_v2_complete")
        .await
        .unwrap();
    db.execute_unprepared("UPDATE zip_v2_executions SET lease_until=datetime('now','-1 minute') WHERE state='admitted'").await.unwrap();
    let retry = complete(&harness, &id, None).await;
    let status = retry.status();
    let body = retry.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(count(&harness, "multipart_uploads").await, 0);
    assert_eq!(count(&harness, "objects").await, 1);
    assert_eq!(harness.captured_add_file_bytes().len(), 2);
}

#[tokio::test]
async fn first_complete_freezes_exact_checksum_contract_before_network_io() {
    let (harness, id, _) = setup().await;
    let invalid = complete(&harness, &id, Some("not-a-checksum")).await;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    assert_eq!(harness.captured_add_file_bytes().len(), 0);
    let changed = complete(&harness, &id, None).await;
    assert_eq!(changed.status(), StatusCode::CONFLICT);
    assert_eq!(count(&harness, "multipart_uploads").await, 1);
    assert_eq!(count(&harness, "objects").await, 0);
    assert_eq!(harness.captured_add_file_bytes().len(), 0);
}

#[tokio::test]
async fn lost_output_guard_fences_retry_without_publishing_any_batch_output() {
    let (harness, id, _) = setup().await;
    let db = harness.state.store.db();
    db.execute_unprepared("CREATE TRIGGER block_v2_complete BEFORE DELETE ON multipart_uploads BEGIN SELECT RAISE(ABORT, 'forced publication rollback'); END").await.unwrap();
    assert_ne!(complete(&harness, &id, None).await.status(), StatusCode::OK);
    db.execute_unprepared("DROP TRIGGER block_v2_complete")
        .await
        .unwrap();
    let changed = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "out/file.txt",
        &[],
        b"successor".to_vec(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(changed.status(), StatusCode::OK);
    db.execute_unprepared("UPDATE zip_v2_executions SET lease_until=datetime('now','-1 minute') WHERE state='admitted'").await.unwrap();
    let retry = complete(&harness, &id, None).await;
    assert_eq!(
        retry.status(),
        StatusCode::CONFLICT,
        "{}",
        retry.text().await.unwrap()
    );
    assert_eq!(
        store::zip::execution::read(db, &id)
            .await
            .unwrap()
            .unwrap()
            .state,
        "fenced"
    );
    assert_eq!(
        store::object::get_latest(db, &harness.bucket, "out/file.txt")
            .await
            .unwrap()
            .cid,
        "QmReplacement"
    );
    assert_eq!(count(&harness, "objects").await, 1);
    assert_eq!(count(&harness, "multipart_uploads").await, 1);
    assert_eq!(harness.captured_add_file_bytes().len(), 3);
}

#[tokio::test]
async fn sqlite_migration_refuses_to_drop_committed_complete_receipt() {
    use sea_orm_migration::{MigrationTrait, SchemaManager};
    let (harness, id, _) = setup().await;
    assert_eq!(complete(&harness, &id, None).await.status(), StatusCode::OK);
    let manager = SchemaManager::new(harness.state.store.db());
    let migration = store::migrations::m20260927_000007_zip_v2_mpu_completion::Migration;
    assert!(migration.down(&manager).await.is_err());
    assert_eq!(count(&harness, "zip_v2_mpu_completions").await, 1);
}

#[tokio::test]
#[ignore = "requires explicitly authorized isolated PostgreSQL via IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_migration_creates_completion_receipt_table_in_isolated_schema() {
    use sea_orm::{ConnectOptions, Database};
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL").expect("set isolated PostgreSQL URL");
    let admin = Database::connect(&url).await.unwrap();
    let schema = format!("zip_v2_complete_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let result = async {
        let mut options = ConnectOptions::new(&url);
        options.max_connections(1).min_connections(1);
        let db = Database::connect(options).await.unwrap();
        db.execute_unprepared(&format!("SET search_path TO {schema}"))
            .await
            .unwrap();
        store::run_migrations(&db).await.unwrap();
        let exists = db
            .query_one(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT to_regclass('zip_v2_mpu_completions')::text AS name".to_owned(),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            exists
                .try_get::<Option<String>>("", "name")
                .unwrap()
                .as_deref(),
            Some("zip_v2_mpu_completions")
        );
        db.close().await.unwrap();
    };
    let outcome = std::panic::AssertUnwindSafe(result).catch_unwind().await;
    admin
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    admin.close().await.unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn late_kubo_cat_trailer_cannot_bind_input_or_finalize_upload_over_http() {
    use std::{collections::HashMap, sync::Arc};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    let archive = legal_single_entry_zip();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let kubo_url = format!("http://{}", listener.local_addr().unwrap());
    let kubo = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let archive = archive.clone();
            tokio::spawn(async move {
                let mut headers = Vec::new();
                while !headers.ends_with(b"\r\n\r\n") {
                    let mut byte = [0_u8; 1];
                    if socket.read_exact(&mut byte).await.is_err() {
                        return;
                    }
                    headers.push(byte[0]);
                }
                if headers.starts_with(b"POST /api/v0/cat?") {
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-Stream-Error\r\nConnection: close\r\n\r\n{:x}\r\n",
                        archive.len()
                    );
                    socket.write_all(response.as_bytes()).await.unwrap();
                    socket.write_all(&archive).await.unwrap();
                    socket
                        .write_all(b"\r\n0\r\nX-Stream-Error: late failure\r\n\r\n")
                        .await
                        .unwrap();
                } else if headers.starts_with(b"POST /api/v0/add?") {
                    let body = b"{\"Hash\":\"QmEarly\",\"Size\":\"0\"}\n";
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    if socket.write_all(response.as_bytes()).await.is_ok() {
                        let _ = socket.write_all(body).await;
                    }
                }
            });
        }
    });
    let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let state = Arc::new(ipfs_s3_gateway::state::AppState {
        kubo: ipfs_s3_gateway::kubo::KuboClient::new(kubo_url),
        cold_kubo: None,
        store: store::Store::new(db),
        credentials: HashMap::from([("test".into(), s3s::auth::SecretKey::from("test"))]),
        master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: ipfs_s3_gateway::pinning::coordinator::PinningCoordinator::disabled_for_test(),
    });
    let bucket = "test-bkt";
    store::bucket::create(state.store.db(), bucket, None)
        .await
        .unwrap();
    let server = support::decompress::start_s3_server(
        state.clone(),
        Arc::new(tokio::sync::Mutex::new(Vec::new())),
    )
    .await;
    let created = support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &server.endpoint,
        bucket,
        "archive.zip",
        &[("uploads", ""), ("decompress-zip", "out/")],
        vec![],
        create_headers("trailer-token"),
        "test",
    )
    .await;
    assert_eq!(created.status(), StatusCode::OK);
    let xml = created.text().await.unwrap();
    let id = xml
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_owned();
    let archive_len = legal_single_entry_zip().len() as i64;
    store::multipart::upsert_part(state.store.db(), &id, 1, "QmPart", archive_len, "QmPart")
        .await
        .unwrap();
    let body = b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"QmPart\"</ETag></Part></CompleteMultipartUpload>".to_vec();
    let result = support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &server.endpoint,
        bucket,
        "archive.zip",
        &[("uploadId", &id)],
        body,
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_ne!(result.status(), StatusCode::OK);
    let execution = store::zip::execution::read(state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(execution.state, "pending");
    assert_eq!(execution.input_sha256, None);
    assert!(
        store::object::get_latest(state.store.db(), bucket, "out/file.txt")
            .await
            .is_err()
    );
    assert!(
        store::multipart::get_upload(state.store.db(), &id)
            .await
            .is_ok()
    );
    kubo.abort();
}

#[tokio::test]
async fn different_authenticated_owner_cannot_replay_original_upload() {
    use std::{collections::HashMap, sync::Arc};
    let (harness, id, _) = setup().await;
    assert_eq!(complete(&harness, &id, None).await.status(), StatusCode::OK);
    let state = Arc::new(ipfs_s3_gateway::state::AppState {
        kubo: harness.state.kubo.clone(),
        cold_kubo: None,
        store: harness.state.store.clone(),
        credentials: HashMap::from([("other".into(), s3s::auth::SecretKey::from("other-secret"))]),
        master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: harness.state.pinning.clone(),
    });
    let server =
        support::decompress::start_s3_server(state, Arc::new(tokio::sync::Mutex::new(Vec::new())))
            .await;
    let url = support::sigv4::presign_sigv4_query(
        &reqwest::Method::POST,
        &server.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploadId", &id)],
        "other",
        "other-secret",
        None,
        60,
        chrono::Utc::now(),
    );
    let response = reqwest::Client::new().post(url)
        .body("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"QmPart\"</ETag></Part></CompleteMultipartUpload>")
        .send().await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::CONFLICT,
        "{}",
        response.text().await.unwrap()
    );
    assert_eq!(harness.captured_add_file_bytes().len(), 2);
}
