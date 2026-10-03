//! ZIP v2 MPU source publication through authenticated Create and Complete.
#[allow(dead_code)]
mod support;

use std::{collections::HashMap, sync::Arc};

use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{
    config::Config,
    pinning::{
        config::ValidatedPinningConfig,
        coordinator::normalize_validated_config,
        zip_policy::{ZipOutputRuleConfig, ZipRuleEffect},
    },
    store,
    store::{
        entities::{object_version, pin_lease, remote_pin},
        object_version::BucketVersioningState,
    },
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, EntityTrait, PaginatorTrait, QueryFilter,
    Statement, TransactionTrait,
};
use support::decompress::{
    AddReply, KuboScript, TestHarness, legal_single_entry_zip, start_harness_with_root_default,
    start_kubo_harness,
};

#[tokio::test]
async fn source_only_publishes_the_verified_archive_and_replays_its_exact_receipt() {
    let archive = legal_single_entry_zip();
    let harness = start_harness_with_root_default(
        KuboScript {
            add_replies: vec![AddReply::Ok("QmArchive")],
            cat_bodies: [("QmPart".to_owned(), archive.clone())].into(),
        },
        true,
    )
    .await;
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "true"),
        ("x-ipfs3-zip-publish-extracted", "false"),
        ("x-ipfs3-zip-targets", "source"),
        ("x-ipfs3-zip-token", "source-only"),
    ] {
        headers.insert(name, HeaderValue::from_str(value).unwrap());
    }
    let create = support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploads", ""), ("decompress-zip", "out/")],
        vec![],
        headers,
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
    let body = b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"QmPart\"</ETag></Part></CompleteMultipartUpload>".to_vec();
    let complete = || async {
        support::sigv4::send_sigv4(
            reqwest::Method::POST,
            &harness.endpoint,
            &harness.bucket,
            "archive.zip",
            &[("uploadId", id)],
            body.clone(),
            HeaderMap::new(),
            "test",
        )
        .await
    };
    let first = complete().await;
    let status = first.status();
    let response_headers = first.headers().clone();
    let xml = first.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{xml}");
    assert!(
        xml.contains("<SourcePublished>true</SourcePublished>"),
        "{xml}"
    );
    assert!(xml.contains("<RootStatus>disabled</RootStatus>"), "{xml}");
    assert_eq!(response_headers[http::header::ETAG], "\"QmArchive\"");
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "archive.zip")
            .await
            .unwrap()
            .cid,
        "QmArchive"
    );
    assert!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "out/file.txt")
            .await
            .is_err()
    );
    let replay = complete().await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(
        replay.headers()[http::header::ETAG],
        response_headers[http::header::ETAG]
    );
    assert_eq!(replay.text().await.unwrap(), xml);
    assert_eq!(harness.captured_add_file_bytes(), vec![archive]);
}

fn source_and_entries_headers(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "true"),
        ("x-ipfs3-zip-publish-extracted", "true"),
        ("x-ipfs3-zip-targets", "none"),
        ("x-ipfs3-zip-token", token),
    ] {
        headers.insert(name, HeaderValue::from_str(value).unwrap());
    }
    headers
}

async fn put(harness: &TestHarness, key: &str, data: &[u8]) -> reqwest::Response {
    support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        key,
        &[],
        data.to_vec(),
        HeaderMap::new(),
        "test",
    )
    .await
}

async fn setup_source_and_entries(
    versioning: BucketVersioningState,
    existing_source: bool,
) -> (TestHarness, String, Vec<u8>) {
    setup_source_and_entries_with_targets(versioning, existing_source, "none").await
}

async fn setup_source_and_entries_with_targets(
    versioning: BucketVersioningState,
    existing_source: bool,
    targets: &str,
) -> (TestHarness, String, Vec<u8>) {
    let archive = legal_single_entry_zip();
    let harness = start_harness_with_root_default(
        KuboScript {
            add_replies: (if existing_source {
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
        false,
    )
    .await;
    if versioning != BucketVersioningState::Unversioned {
        store::bucket::set_versioning_state(harness.state.store.db(), &harness.bucket, versioning)
            .await
            .unwrap();
    }
    if existing_source {
        let response = put(&harness, "archive.zip", b"original archive").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[http::header::ETAG],
            "\"QmExistingSource\""
        );
    }
    let mut headers = source_and_entries_headers("source-and-entries");
    headers.insert(
        "x-ipfs3-zip-targets",
        HeaderValue::from_str(targets).unwrap(),
    );
    let response = support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploads", ""), ("decompress-zip", "out/")],
        vec![],
        headers,
        "test",
    )
    .await;
    let status = response.status();
    let xml = response.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{xml}");
    let id = xml
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

fn complete_body(checksum: Option<&str>) -> Vec<u8> {
    let checksum = checksum
        .map(|value| format!("<ChecksumSHA256>{value}</ChecksumSHA256>"))
        .unwrap_or_default();
    format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"QmPart\"</ETag>{checksum}</Part></CompleteMultipartUpload>"
    )
    .into_bytes()
}

async fn complete_at(
    harness: &TestHarness,
    endpoint: &str,
    id: &str,
    body: Vec<u8>,
    headers: HeaderMap,
) -> reqwest::Response {
    support::sigv4::send_sigv4(
        reqwest::Method::POST,
        endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploadId", id)],
        body,
        headers,
        "test",
    )
    .await
}

async fn complete(harness: &TestHarness, id: &str) -> reqwest::Response {
    complete_at(
        harness,
        &harness.endpoint,
        id,
        complete_body(None),
        HeaderMap::new(),
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

async fn versions(harness: &TestHarness, key: &str) -> u64 {
    object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq(&harness.bucket))
        .filter(object_version::Column::Key.eq(key))
        .count(harness.state.store.db())
        .await
        .unwrap()
}

async fn kubo_cat_calls(harness: &TestHarness) -> usize {
    harness
        .kubo
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|request| request.url.path() == "/api/v0/cat")
        .count()
}

#[tokio::test]
async fn source_and_entries_complete_publishes_real_source_and_output_in_every_versioning_state() {
    for versioning in [
        BucketVersioningState::Unversioned,
        BucketVersioningState::Enabled,
        BucketVersioningState::Suspended,
    ] {
        for existing_source in [false, true] {
            let (harness, id, archive) =
                setup_source_and_entries(versioning, existing_source).await;
            let response = complete(&harness, &id).await;
            let status = response.status();
            let headers = response.headers().clone();
            let xml = response.text().await.unwrap();
            assert_eq!(
                status,
                StatusCode::OK,
                "{versioning:?} {existing_source}: {xml}"
            );
            assert!(
                xml.contains("<SourcePublished>true</SourcePublished>"),
                "{xml}"
            );
            assert!(xml.contains("<PublishedCount>1</PublishedCount>"), "{xml}");
            assert!(xml.contains("<FailedCount>0</FailedCount>"), "{xml}");
            assert!(xml.contains("<RootStatus>disabled</RootStatus>"), "{xml}");
            assert!(xml.contains("<ETag>\"QmArchive\"</ETag>"), "{xml}");
            assert_eq!(headers[http::header::ETAG], "\"QmArchive\"");
            assert_eq!(headers["x-ipfs-s3-zip-batch-id"], id);
            let version = headers.get("x-amz-version-id").map(|v| v.to_str().unwrap());
            match versioning {
                BucketVersioningState::Unversioned => assert_eq!(version, None),
                BucketVersioningState::Enabled => {
                    let version = version.expect("source version on enabled bucket");
                    uuid::Uuid::parse_str(version).unwrap();
                    assert!(xml.contains(&format!("<VersionId>{version}</VersionId>")));
                }
                BucketVersioningState::Suspended => assert_eq!(version, Some("null")),
            }
            assert_eq!(
                xml.contains("<VersionId>null</VersionId>"),
                version == Some("null")
            );
            let source =
                store::object::get_latest(harness.state.store.db(), &harness.bucket, "archive.zip")
                    .await
                    .unwrap();
            assert_eq!(source.cid, "QmArchive");
            assert_eq!(source.size, archive.len() as i64);
            assert_eq!(
                store::object::get_latest(
                    harness.state.store.db(),
                    &harness.bucket,
                    "out/file.txt",
                )
                .await
                .unwrap()
                .cid,
                "QmOutput"
            );
            assert_eq!(
                versions(&harness, "archive.zip").await,
                if existing_source && versioning == BucketVersioningState::Enabled {
                    2
                } else {
                    1
                }
            );
            assert_eq!(versions(&harness, "out/file.txt").await, 1);
            assert_eq!(count(&harness, "multipart_uploads").await, 0);
            assert_eq!(count(&harness, "multipart_parts").await, 0);
            assert_eq!(count(&harness, "zip_v2_mpu_completions").await, 1);
            let expected_adds = 2 + usize::from(existing_source);
            let adds = harness.captured_add_file_bytes();
            assert_eq!(adds.len(), expected_adds);
            assert_eq!(adds[usize::from(existing_source)], archive);
            let cat_calls = kubo_cat_calls(&harness).await;
            let replay = complete(&harness, &id).await;
            assert_eq!(replay.status(), StatusCode::OK);
            assert_eq!(
                replay.headers()[http::header::ETAG],
                headers[http::header::ETAG]
            );
            assert_eq!(
                replay.headers().get("x-amz-version-id"),
                headers.get("x-amz-version-id")
            );
            assert_eq!(replay.text().await.unwrap(), xml);
            assert_eq!(harness.captured_add_file_bytes().len(), expected_adds);
            assert_eq!(kubo_cat_calls(&harness).await, cat_calls);
        }
    }
}

async fn rollback_before_source_and_entries_publication(harness: &TestHarness, id: &str) {
    let db = harness.state.store.db();
    db.execute_unprepared("CREATE TRIGGER block_source_complete BEFORE DELETE ON multipart_uploads BEGIN SELECT RAISE(ABORT, 'forced publication rollback'); END")
        .await.unwrap();
    let first = complete(harness, id).await;
    assert_ne!(first.status(), StatusCode::OK);
    assert_eq!(count(harness, "multipart_uploads").await, 1);
    assert_eq!(count(harness, "multipart_parts").await, 1);
    assert_eq!(count(harness, "zip_v2_targets").await, 1);
    assert_eq!(count(harness, "objects").await, 0);
    assert_eq!(count(harness, "object_versions").await, 0);
    assert_eq!(count(harness, "zip_v2_mpu_completions").await, 1);
    assert_eq!(harness.captured_add_file_bytes().len(), 2);
    db.execute_unprepared("DROP TRIGGER block_source_complete")
        .await
        .unwrap();
}

async fn expire_admitted_lease(harness: &TestHarness) {
    harness.state.store.db().execute_unprepared(
        "UPDATE zip_v2_executions SET lease_until=datetime('now','-1 minute') WHERE state='admitted'",
    ).await.unwrap();
}

#[tokio::test]
async fn source_and_entries_rollback_retries_retained_manifest_and_replays_exact_receipt_after_restart()
 {
    let (harness, id, archive) =
        setup_source_and_entries(BucketVersioningState::Enabled, false).await;
    rollback_before_source_and_entries_publication(&harness, &id).await;
    let cat_calls = kubo_cat_calls(&harness).await;
    expire_admitted_lease(&harness).await;
    let restarted_state = Arc::new(ipfs_s3_gateway::state::AppState {
        kubo: harness.state.kubo.clone(),
        cold_kubo: None,
        store: harness.state.store.clone(),
        credentials: HashMap::from([("test".into(), s3s::auth::SecretKey::from("test"))]),
        master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: harness.state.pinning.clone(),
    });
    let restarted = support::decompress::start_s3_server(
        restarted_state,
        Arc::new(tokio::sync::Mutex::new(Vec::new())),
    )
    .await;
    let retry = complete_at(
        &harness,
        &restarted.endpoint,
        &id,
        complete_body(None),
        HeaderMap::new(),
    )
    .await;
    let status = retry.status();
    let headers = retry.headers().clone();
    let xml = retry.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{xml}");
    assert!(
        xml.contains("<SourcePublished>true</SourcePublished>"),
        "{xml}"
    );
    assert!(xml.contains("<PublishedCount>1</PublishedCount>"), "{xml}");
    assert_eq!(headers[http::header::ETAG], "\"QmArchive\"");
    let version = headers["x-amz-version-id"].to_str().unwrap();
    uuid::Uuid::parse_str(version).unwrap();
    assert!(xml.contains(&format!("<VersionId>{version}</VersionId>")));
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "archive.zip")
            .await
            .unwrap()
            .cid,
        "QmArchive"
    );
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "out/file.txt")
            .await
            .unwrap()
            .cid,
        "QmOutput"
    );
    assert_eq!(count(&harness, "multipart_uploads").await, 0);
    assert_eq!(count(&harness, "multipart_parts").await, 0);
    assert_eq!(versions(&harness, "archive.zip").await, 1);
    assert_eq!(versions(&harness, "out/file.txt").await, 1);
    assert_eq!(
        harness.captured_add_file_bytes(),
        vec![archive, b"single entry bytes".to_vec()]
    );
    assert_eq!(kubo_cat_calls(&harness).await, cat_calls);
    let overwrite = put(&harness, "archive.zip", b"successor").await;
    assert_eq!(overwrite.status(), StatusCode::OK);
    let replay = complete_at(
        &harness,
        &restarted.endpoint,
        &id,
        complete_body(None),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(
        replay.headers()[http::header::ETAG],
        headers[http::header::ETAG]
    );
    assert_eq!(
        replay.headers()["x-amz-version-id"],
        headers["x-amz-version-id"]
    );
    assert_eq!(replay.text().await.unwrap(), xml);
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "archive.zip")
            .await
            .unwrap()
            .cid,
        "QmReplacement"
    );
    assert_eq!(harness.captured_add_file_bytes().len(), 3);
    assert_eq!(kubo_cat_calls(&harness).await, cat_calls);
}

async fn lost_guard_fences_retained_source_and_entries_retry(key: &str) {
    let (harness, id, _) =
        setup_source_and_entries(BucketVersioningState::Unversioned, false).await;
    rollback_before_source_and_entries_publication(&harness, &id).await;
    let cat_calls = kubo_cat_calls(&harness).await;
    let overwrite = put(&harness, key, b"successor").await;
    assert_eq!(overwrite.status(), StatusCode::OK);
    expire_admitted_lease(&harness).await;
    let retry = complete(&harness, &id).await;
    let status = retry.status();
    let xml = retry.text().await.unwrap();
    assert_eq!(status, StatusCode::CONFLICT, "{xml}");
    assert_eq!(
        store::zip::execution::read(harness.state.store.db(), &id)
            .await
            .unwrap()
            .unwrap()
            .state,
        "fenced"
    );
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
            .await
            .unwrap()
            .cid,
        "QmReplacement"
    );
    let other = if key == "archive.zip" {
        "out/file.txt"
    } else {
        "archive.zip"
    };
    assert!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, other)
            .await
            .is_err()
    );
    assert_eq!(count(&harness, "objects").await, 1);
    assert_eq!(count(&harness, "object_versions").await, 1);
    assert_eq!(count(&harness, "multipart_uploads").await, 1);
    assert_eq!(harness.captured_add_file_bytes().len(), 3);
    assert_eq!(kubo_cat_calls(&harness).await, cat_calls);
}

#[tokio::test]
async fn lost_source_guard_fences_retained_retry_before_publishing_extracted_output() {
    lost_guard_fences_retained_source_and_entries_retry("archive.zip").await;
}

#[tokio::test]
async fn lost_output_guard_fences_retained_retry_before_publishing_source() {
    lost_guard_fences_retained_source_and_entries_retry("out/file.txt").await;
}

#[tokio::test]
async fn source_and_entries_complete_rejects_conflicting_part_contract_and_token_without_new_io() {
    use base64::Engine as _;
    use sha2::{Digest, Sha256};

    let (harness, id, archive) =
        setup_source_and_entries(BucketVersioningState::Enabled, false).await;
    let checksum = base64::engine::general_purpose::STANDARD.encode(Sha256::digest(&archive));
    let original_body = complete_body(Some(&checksum));
    let first = complete_at(
        &harness,
        &harness.endpoint,
        &id,
        original_body.clone(),
        HeaderMap::new(),
    )
    .await;
    let status = first.status();
    let headers = first.headers().clone();
    let xml = first.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{xml}");
    let cat_calls = kubo_cat_calls(&harness).await;
    let changed = complete(&harness, &id).await;
    assert_eq!(changed.status(), StatusCode::CONFLICT);
    let mut wrong_token = HeaderMap::new();
    wrong_token.insert(
        "x-ipfs3-zip-token",
        HeaderValue::from_static("different-token"),
    );
    let changed = complete_at(
        &harness,
        &harness.endpoint,
        &id,
        original_body.clone(),
        wrong_token,
    )
    .await;
    assert_eq!(changed.status(), StatusCode::CONFLICT);
    let replay = complete_at(
        &harness,
        &harness.endpoint,
        &id,
        original_body,
        HeaderMap::new(),
    )
    .await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(
        replay.headers()[http::header::ETAG],
        headers[http::header::ETAG]
    );
    assert_eq!(
        replay.headers()["x-amz-version-id"],
        headers["x-amz-version-id"]
    );
    assert_eq!(replay.text().await.unwrap(), xml);
    assert_eq!(count(&harness, "zip_v2_mpu_completions").await, 1);
    assert_eq!(versions(&harness, "archive.zip").await, 1);
    assert_eq!(versions(&harness, "out/file.txt").await, 1);
    assert_eq!(harness.captured_add_file_bytes().len(), 2);
    assert_eq!(kubo_cat_calls(&harness).await, cat_calls);
}

#[tokio::test]
async fn source_and_entries_failed_root_keeps_both_real_objects_without_claiming_a_root() {
    let archive = legal_single_entry_zip();
    let harness = start_harness_with_root_default(
        KuboScript {
            add_replies: vec![AddReply::Ok("QmArchive"), AddReply::Ok("QmOutput")],
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
        source_and_entries_headers("source-root-failed"),
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
    let result = complete(&harness, id).await;
    let status = result.status();
    let headers = result.headers().clone();
    let xml = result.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{xml}");
    assert!(
        xml.contains("<SourcePublished>true</SourcePublished>"),
        "{xml}"
    );
    assert!(xml.contains("<PublishedCount>1</PublishedCount>"), "{xml}");
    assert!(xml.contains("<RootStatus>failed</RootStatus>"), "{xml}");
    assert!(!headers.contains_key("x-ipfs-s3-zip-root-cid"));
    assert_eq!(headers[http::header::ETAG], "\"QmArchive\"");
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "archive.zip")
            .await
            .unwrap()
            .cid,
        "QmArchive"
    );
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "out/file.txt")
            .await
            .unwrap()
            .cid,
        "QmOutput"
    );
    assert_eq!(count(&harness, "multipart_uploads").await, 0);
    let replay = complete(&harness, id).await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.text().await.unwrap(), xml);
    assert_eq!(harness.captured_add_file_bytes().len(), 2);

    // Root-only recovery operates on the immutable publication binding, not on
    // today's source key or its already-completed mutation guard.
    let deleted = support::sigv4::send_sigv4(
        reqwest::Method::DELETE,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[],
        vec![],
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    let db = harness.state.store.db();
    let snapshot = store::zip::snapshot(db, id).await.unwrap().unwrap();
    let terminal = snapshot.batch.terminal_result.unwrap();
    db.execute_unprepared("UPDATE zip_root_builds SET lease_until=datetime('now','-1 minute')")
        .await
        .unwrap();
    let claim = store::zip::claim_root(db, id, "root-only-retry", 60)
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    let mut forged: serde_json::Value = serde_json::from_str(&terminal).unwrap();
    forged["source_version_row_id"] = "different-source-version".into();
    assert!(
        claim
            .settle_failed_retry(&tx, &forged.to_string(), "root_build_failed")
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    let tx = db.begin().await.unwrap();
    claim
        .settle_failed_retry(&tx, &terminal, "root_build_failed")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(versions(&harness, "archive.zip").await, 0);
    assert_eq!(versions(&harness, "out/file.txt").await, 1);
    let replay = complete(&harness, id).await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.text().await.unwrap(), xml);
    assert_eq!(harness.captured_add_file_bytes().len(), 2);
}

#[tokio::test]
async fn source_and_entries_targets_intersect_per_output_deny_and_do_not_copy_archive_policy() {
    for (targets, expected_remote) in [("both", 1), ("extracted", 0)] {
        let archive = legal_single_entry_zip();
        let kubo = start_kubo_harness(KuboScript {
            add_replies: vec![AddReply::Ok("QmArchive"), AddReply::Ok("QmOutput")],
            cat_bodies: [
                ("QmPart".to_owned(), archive.clone()),
                ("QmArchive".to_owned(), archive.clone()),
            ]
            .into(),
        })
        .await;
        let raw = format!(
            r#"
            [kubo]
            rpc_url = {:?}
            [storage]
            database_url = "sqlite::memory:"
            [decompress_zip]
            unixfs_directory_root = false
            [[pinning.providers]]
            name = "alpha"
            kind = "noop"
            priority = 1
            max_bytes = 1000000
            max_pins = 100
            [[pinning.policies]]
            bucket = "test-bkt"
            prefix = ""
            trigger = "always"
            provider_mode = "one"
            providers = ["alpha"]
            default_duration = "1h"
            max_duration = "2h"
            allow_decompressed = true
        "#,
            kubo.server.uri()
        );
        let mut cfg: Config = toml::from_str(&raw).unwrap();
        let policy = normalize_validated_config(
            ValidatedPinningConfig::from_config(&cfg, |_| None).unwrap(),
        )
        .unwrap()
        .policies[0]
            .identity
            .clone();
        cfg.decompress_zip.pin_output_rules = vec![
            ZipOutputRuleConfig {
                name: "archive-and-entries".into(),
                priority: 1,
                bucket: "test-bkt".into(),
                prefix: "".into(),
                effect: ZipRuleEffect::Allow,
                policy_id: Some(policy),
            },
            ZipOutputRuleConfig {
                name: "private-entry".into(),
                priority: 100,
                bucket: "test-bkt".into(),
                prefix: "out/file.txt".into(),
                effect: ZipRuleEffect::Deny,
                policy_id: None,
            },
        ];
        let state = ipfs_s3_gateway::state::AppState::new(&cfg).await.unwrap();
        store::bucket::create(state.store.db(), "test-bkt", None)
            .await
            .unwrap();
        let server = support::decompress::start_s3_server(
            state.clone(),
            Arc::new(tokio::sync::Mutex::new(Vec::new())),
        )
        .await;
        let mut headers = source_and_entries_headers("remote-intersection");
        headers.insert(
            "x-ipfs3-zip-targets",
            HeaderValue::from_str(targets).unwrap(),
        );
        let create = support::sigv4::send_sigv4(
            reqwest::Method::POST,
            &server.endpoint,
            "test-bkt",
            "archive.zip",
            &[("uploads", ""), ("decompress-zip", "out/")],
            vec![],
            headers,
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
            .unwrap();
        store::multipart::upsert_part(
            state.store.db(),
            id,
            1,
            "QmPart",
            archive.len() as i64,
            "QmPart",
        )
        .await
        .unwrap();
        let response = support::sigv4::send_sigv4(
            reqwest::Method::POST,
            &server.endpoint,
            "test-bkt",
            "archive.zip",
            &[("uploadId", id)],
            complete_body(None),
            HeaderMap::new(),
            "test",
        )
        .await;
        let status = response.status();
        let xml = response.text().await.unwrap();
        assert_eq!(status, StatusCode::OK, "{targets}: {xml}");
        let source = store::object::get_latest(state.store.db(), "test-bkt", "archive.zip")
            .await
            .unwrap();
        assert_eq!(
            store::object::get_latest(state.store.db(), "test-bkt", "out/file.txt")
                .await
                .unwrap()
                .cid,
            "QmOutput"
        );
        let leases = pin_lease::Entity::find()
            .all(state.store.db())
            .await
            .unwrap();
        assert_eq!(leases.len(), expected_remote);
        assert!(
            leases
                .iter()
                .all(|lease| lease.owner_object_id == source.id && lease.source == "automatic")
        );
        let remotes = remote_pin::Entity::find()
            .all(state.store.db())
            .await
            .unwrap();
        assert_eq!(remotes.len(), expected_remote);
        assert!(remotes.iter().all(|remote| remote.cid == "QmArchive"));
    }
}

#[tokio::test]
async fn changed_remote_rules_after_create_reject_source_complete_before_archive_io() {
    let (harness, id, _) =
        setup_source_and_entries_with_targets(BucketVersioningState::Unversioned, false, "both")
            .await;
    let raw = format!(
        r#"
        [kubo]
        rpc_url = {:?}
        [storage]
        database_url = "sqlite::memory:"
        [[decompress_zip.pin_output_rules]]
        name = "new-private-rule"
        priority = 1
        bucket = "test-bkt"
        prefix = "out/"
        effect = "deny"
    "#,
        harness.state.kubo.base_url()
    );
    let cfg: Config = toml::from_str(&raw).unwrap();
    let changed = ipfs_s3_gateway::state::AppState::new(&cfg).await.unwrap();
    let state = Arc::new(ipfs_s3_gateway::state::AppState {
        kubo: harness.state.kubo.clone(),
        cold_kubo: None,
        store: harness.state.store.clone(),
        credentials: HashMap::from([("test".into(), s3s::auth::SecretKey::from("test"))]),
        master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: changed.pinning.clone(),
    });
    let server =
        support::decompress::start_s3_server(state, Arc::new(tokio::sync::Mutex::new(Vec::new())))
            .await;
    let first = support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &server.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploadId", &id)],
        complete_body(None),
        HeaderMap::new(),
        "test",
    )
    .await;
    let status = first.status();
    let xml = first.text().await.unwrap();
    assert_eq!(status, StatusCode::CONFLICT, "{xml}");
    assert_eq!(harness.captured_add_file_bytes().len(), 0);
    assert_eq!(kubo_cat_calls(&harness).await, 0);
    assert_eq!(count(&harness, "objects").await, 0);
    assert_eq!(count(&harness, "multipart_uploads").await, 1);
}

#[tokio::test]
async fn source_publishes_when_every_entry_fails_without_fabricating_a_root() {
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
    let created = support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploads", ""), ("decompress-zip", "out/")],
        vec![],
        source_and_entries_headers("source-with-failed-entry"),
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
    let result = complete(&harness, id).await;
    let status = result.status();
    let headers = result.headers().clone();
    let xml = result.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{xml}");
    assert!(
        xml.contains("<SourcePublished>true</SourcePublished>"),
        "{xml}"
    );
    assert!(
        xml.contains("<BatchStatus>completed</BatchStatus>"),
        "{xml}"
    );
    assert!(xml.contains("<PublishedCount>0</PublishedCount>"), "{xml}");
    assert!(xml.contains("<FailedCount>1</FailedCount>"), "{xml}");
    assert!(xml.contains("<RootStatus>empty</RootStatus>"), "{xml}");
    assert!(!headers.contains_key("x-ipfs-s3-zip-root-cid"));
    assert_eq!(headers[http::header::ETAG], "\"QmArchive\"");
    assert_eq!(count(&harness, "objects").await, 1);
    let replay = complete(&harness, id).await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.text().await.unwrap(), xml);
}

#[tokio::test]
async fn source_true_empty_zip_publishes_the_real_source_and_returns_200_without_a_root() {
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
    let created = support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploads", ""), ("decompress-zip", "out/")],
        vec![],
        source_and_entries_headers("real-empty-source"),
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
    let response = complete(&harness, id).await;
    let status = response.status();
    let headers = response.headers().clone();
    let xml = response.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{xml}");
    assert!(xml.contains("<SourcePublished>true</SourcePublished>"));
    assert!(xml.contains("<BatchStatus>completed</BatchStatus>"));
    assert!(xml.contains("<PublishedCount>0</PublishedCount>"));
    assert!(xml.contains("<RootStatus>empty</RootStatus>"));
    assert!(!headers.contains_key("x-ipfs-s3-zip-root-cid"));
    assert_eq!(headers[http::header::ETAG], "\"QmEmptyArchive\"");
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "archive.zip")
            .await
            .unwrap()
            .cid,
        "QmEmptyArchive"
    );
    assert_eq!(count(&harness, "objects").await, 1);
    let adds = harness.captured_add_file_bytes();
    let replay = complete(&harness, id).await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(
        replay.headers()[http::header::ETAG],
        headers[http::header::ETAG]
    );
    assert_eq!(replay.text().await.unwrap(), xml);
    assert_eq!(harness.captured_add_file_bytes(), adds);
}

#[tokio::test]
async fn sqlite_rejects_forged_source_published_for_captured_source_false_and_http_keeps_s_private()
{
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
        false,
    )
    .await;
    let mut controls = source_and_entries_headers("forged-source-flag");
    controls.insert(
        "x-ipfs3-zip-publish-source",
        HeaderValue::from_static("false"),
    );
    let created = support::sigv4::send_sigv4(
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
    let db = harness.state.store.db();
    db.execute_unprepared("CREATE TRIGGER block_forged_complete BEFORE DELETE ON multipart_uploads BEGIN SELECT RAISE(ABORT, 'defer final publication'); END")
        .await.unwrap();
    assert_ne!(complete(&harness, &id).await.status(), StatusCode::OK);
    db.execute_unprepared("DROP TRIGGER block_forged_complete")
        .await
        .unwrap();
    assert_eq!(count(&harness, "zip_v2_targets").await, 0);
    let snapshot = store::zip::execution::read(db, &id).await.unwrap().unwrap();
    let tx = db.begin().await.unwrap();
    let terminal = serde_json::json!({
        "input_sha256": snapshot.input_sha256.unwrap(),
        "published_count": 0,
        "failed_count": 1,
        "status": "failed",
    })
    .to_string();
    let forged = store::zip::publish(
        &tx,
        &id,
        &[],
        true,
        &terminal,
        store::zip::RootOutcome::Disabled,
    )
    .await;
    assert!(
        forged.is_err(),
        "captured source=false must never accept source_published=true"
    );
    tx.rollback().await.unwrap();
    assert_eq!(count(&harness, "objects").await, 0);
    assert_eq!(count(&harness, "object_versions").await, 0);
    assert_eq!(count(&harness, "multipart_uploads").await, 1);
    expire_admitted_lease(&harness).await;
    let result = complete(&harness, &id).await;
    let status = result.status();
    let headers = result.headers().clone();
    let xml = result.text().await.unwrap();
    assert_eq!(status, StatusCode::BAD_REQUEST, "{xml}");
    assert!(xml.contains("<Code>InvalidRequest</Code>"));
    assert!(!headers.contains_key(http::header::ETAG));
    assert!(!headers.contains_key("x-amz-version-id"));
    assert!(
        store::object::get_latest(db, &harness.bucket, "archive.zip")
            .await
            .is_err()
    );
    assert_eq!(versions(&harness, "archive.zip").await, 0);
    assert_eq!(versions(&harness, "out/file.txt").await, 0);
}

#[tokio::test]
async fn source_true_without_its_captured_exact_guard_fences_complete_without_writes() {
    let (harness, id, _) = setup_source_and_entries(BucketVersioningState::Enabled, false).await;
    rollback_before_source_and_entries_publication(&harness, &id).await;
    let db = harness.state.store.db();
    db.execute_unprepared(
        "UPDATE standard_mutation_leases SET lease_until=datetime('now','-1 minute') WHERE bucket='test-bkt' AND key='archive.zip'",
    ).await.unwrap();
    expire_admitted_lease(&harness).await;
    let adds = harness.captured_add_file_bytes().len();
    let cats = kubo_cat_calls(&harness).await;
    let result = complete(&harness, &id).await;
    assert_eq!(result.status(), StatusCode::CONFLICT);
    assert_eq!(
        store::zip::execution::read(db, &id)
            .await
            .unwrap()
            .unwrap()
            .state,
        "fenced"
    );
    assert_eq!(count(&harness, "objects").await, 0);
    assert_eq!(count(&harness, "object_versions").await, 0);
    assert_eq!(count(&harness, "multipart_uploads").await, 1);
    assert_eq!(harness.captured_add_file_bytes().len(), adds);
    assert_eq!(kubo_cat_calls(&harness).await, cats);
}

#[tokio::test]
async fn changed_verified_source_cid_or_size_inside_publication_rolls_back_every_version_and_receipt()
 {
    for changed in ["input_art_cid='QmNotTheArchive'", "input_art_size=1"] {
        let (harness, id, _) =
            setup_source_and_entries(BucketVersioningState::Enabled, false).await;
        let db = harness.state.store.db();
        db.execute_unprepared(&format!(
            "CREATE TRIGGER corrupt_verified_source AFTER INSERT ON object_versions WHEN NEW.key='archive.zip' BEGIN UPDATE zip_v2_executions SET {changed} WHERE id='{id}'; END"
        )).await.unwrap();
        let result = complete(&harness, &id).await;
        assert_ne!(result.status(), StatusCode::OK, "{changed}");
        assert_eq!(count(&harness, "objects").await, 0, "{changed}");
        assert_eq!(count(&harness, "object_versions").await, 0, "{changed}");
        assert_eq!(count(&harness, "pin_leases").await, 0, "{changed}");
        assert_eq!(count(&harness, "multipart_uploads").await, 1, "{changed}");
        let execution = store::zip::execution::read(db, &id).await.unwrap().unwrap();
        assert_eq!(execution.state, "admitted", "{changed}");
        assert_eq!(
            execution.input_art_cid.as_deref(),
            Some("QmArchive"),
            "{changed}"
        );
        assert_eq!(count(&harness, "zip_v2_mpu_completions").await, 1);
        db.execute_unprepared("DROP TRIGGER corrupt_verified_source")
            .await
            .unwrap();
        expire_admitted_lease(&harness).await;
        let retry = complete(&harness, &id).await;
        let status = retry.status();
        let headers = retry.headers().clone();
        let xml = retry.text().await.unwrap();
        assert_eq!(status, StatusCode::OK, "{changed}: {xml}");
        assert!(xml.contains("<SourcePublished>true</SourcePublished>"));
        assert_eq!(headers[http::header::ETAG], "\"QmArchive\"");
        assert_eq!(versions(&harness, "archive.zip").await, 1);
        assert_eq!(versions(&harness, "out/file.txt").await, 1);
    }
}

#[tokio::test]
async fn source_binding_rejects_a_marker_without_the_real_published_source_version() {
    let (harness, id, _) = setup_source_and_entries(BucketVersioningState::Enabled, false).await;
    rollback_before_source_and_entries_publication(&harness, &id).await;
    let db = harness.state.store.db();
    let execution = store::zip::execution::read(db, &id).await.unwrap().unwrap();
    let terminal = serde_json::json!({
        "input_sha256": execution.input_sha256,
        "source_cid": execution.input_art_cid,
        "source_size": execution.input_art_size,
        "source_version_id": "forged-public-version",
        "source_version_row_id": "missing-private-version",
        "published_count": 1, "failed_count": 0, "status": "completed",
    })
    .to_string();
    // Retain an empty successful manifest so the archive binding is the only
    // possible rejection; transaction rollback restores the real output mirror.
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared("DELETE FROM zip_manifest_entries")
        .await
        .unwrap();
    let result = store::zip::publish(
        &tx,
        &id,
        &[],
        true,
        &terminal,
        store::zip::RootOutcome::Disabled,
    )
    .await;
    assert!(
        result.is_err(),
        "a source marker is not a real published source version"
    );
    tx.rollback().await.unwrap();
    expire_admitted_lease(&harness).await;
    assert_eq!(complete(&harness, &id).await.status(), StatusCode::OK);
    assert_eq!(harness.captured_add_file_bytes().len(), 2);
}

#[tokio::test]
async fn signed_complete_rejects_corrupted_real_source_object_or_public_version_and_rolls_back() {
    for (stage, corruption) in [
        (
            "AFTER INSERT ON object_versions WHEN NEW.key='archive.zip'",
            "UPDATE objects SET cid='QmForgedSource' WHERE id=NEW.object_id;",
        ),
        (
            "AFTER INSERT ON object_versions WHEN NEW.key='archive.zip'",
            "UPDATE objects SET size=1 WHERE id=NEW.object_id;",
        ),
        (
            "AFTER INSERT ON object_versions WHEN NEW.key='archive.zip'",
            "UPDATE object_versions SET version_id='forged-public-version' WHERE id=NEW.id;",
        ),
        (
            "AFTER UPDATE OF state ON zip_v2_executions WHEN NEW.state='completed'",
            "UPDATE objects SET cid='QmForgedSource' WHERE key='archive.zip';",
        ),
    ] {
        let (harness, id, _) =
            setup_source_and_entries(BucketVersioningState::Enabled, false).await;
        let db = harness.state.store.db();
        db.execute_unprepared(&format!(
            "CREATE TRIGGER corrupt_source_binding {stage} BEGIN {corruption} END"
        ))
        .await
        .unwrap();
        let result = complete(&harness, &id).await;
        let status = result.status();
        let xml = result.text().await.unwrap();
        assert_ne!(status, StatusCode::OK, "{corruption}: {xml}");
        assert_eq!(count(&harness, "objects").await, 0);
        assert_eq!(count(&harness, "object_versions").await, 0);
        assert_eq!(count(&harness, "multipart_uploads").await, 1);
        db.execute_unprepared("DROP TRIGGER corrupt_source_binding")
            .await
            .unwrap();
        expire_admitted_lease(&harness).await;
        let retry = complete(&harness, &id).await;
        assert_eq!(retry.status(), StatusCode::OK);
        assert_eq!(retry.headers()[http::header::ETAG], "\"QmArchive\"");
        assert_eq!(harness.captured_add_file_bytes().len(), 2);
    }
}

#[tokio::test]
async fn signed_complete_does_not_accept_an_unverified_crc64nvme_checksum() {
    let (harness, id, _) = setup_source_and_entries(BucketVersioningState::Enabled, false).await;
    let body = String::from_utf8(complete_body(None)).unwrap().replace(
        "</Part>",
        "<ChecksumCRC64NVME>AAAAAAAAAAA=</ChecksumCRC64NVME></Part>",
    );
    let result = complete_at(
        &harness,
        &harness.endpoint,
        &id,
        body.into_bytes(),
        HeaderMap::new(),
    )
    .await;
    let status = result.status();
    let xml = result.text().await.unwrap();
    assert_eq!(status, StatusCode::BAD_REQUEST, "{xml}");
    assert!(harness.captured_add_file_bytes().is_empty());
    assert_eq!(kubo_cat_calls(&harness).await, 0);
    assert_eq!(count(&harness, "objects").await, 0);
}

#[tokio::test]
async fn signed_complete_replays_committed_receipt_after_client_discards_response_and_gateway_restarts()
 {
    let (harness, id, _) = setup_source_and_entries(BucketVersioningState::Enabled, false).await;
    let response = complete(&harness, &id).await;
    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers().clone();
    drop(response); // Client has no XML result; retry must resolve the durable commit.
    let row = harness
        .state
        .store
        .db()
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT response_xml FROM zip_v2_mpu_completions WHERE upload_id=?",
            [id.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap();
    let xml: String = row.try_get("", "response_xml").unwrap();
    let adds = harness.captured_add_file_bytes();
    let cats = kubo_cat_calls(&harness).await;
    let restarted = support::decompress::start_s3_server(
        harness.state.clone(),
        Arc::new(tokio::sync::Mutex::new(Vec::new())),
    )
    .await;
    let replay = complete_at(
        &harness,
        &restarted.endpoint,
        &id,
        complete_body(None),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(replay.status(), StatusCode::OK);
    for name in [
        "etag",
        "x-amz-version-id",
        "x-ipfs-s3-zip-batch-id",
        "x-ipfs-s3-zip-batch-status",
        "x-ipfs-s3-zip-root-status",
    ] {
        assert_eq!(replay.headers().get(name), headers.get(name));
    }
    assert_eq!(replay.text().await.unwrap(), xml);
    assert_eq!(harness.captured_add_file_bytes(), adds);
    assert_eq!(kubo_cat_calls(&harness).await, cats);
    assert_eq!(count(&harness, "object_versions").await, 2);
    assert_eq!(count(&harness, "multipart_uploads").await, 0);
    assert_eq!(count(&harness, "multipart_parts").await, 0);
}
