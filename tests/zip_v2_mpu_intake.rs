//! Authenticated CreateMultipartUpload ZIP v2 intake; no Complete assertions.
#[allow(dead_code)]
mod support;
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{store, zip::options::ZipV2Options};
use sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement};
use support::decompress::{KuboScript, start_harness_with_root_default_and_database};

fn controls(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "false"),
        ("x-ipfs3-zip-publish-extracted", "true"),
        ("x-ipfs3-zip-targets", "extracted"),
        ("x-ipfs3-zip-token", token),
    ] {
        headers.insert(name, HeaderValue::from_str(value).unwrap());
    }
    headers
}

async fn create(
    harness: &support::decompress::TestHarness,
    query: &[(&str, &str)],
    headers: HeaderMap,
) -> reqwest::Response {
    support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        query,
        vec![],
        headers,
        "test",
    )
    .await
}

async fn setup() -> (
    tempfile::TempDir,
    support::decompress::TestHarness,
    sea_orm::DatabaseConnection,
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join("v2-intake.sqlite")
        .display()
        .to_string()
        .replace('\\', "/");
    let url = format!("sqlite://{path}?mode=rwc");
    let harness = start_harness_with_root_default_and_database(
        KuboScript {
            add_replies: vec![],
            cat_bodies: Default::default(),
        },
        false,
        &url,
    )
    .await;
    let separate = Database::connect(&url).await.unwrap();
    separate
        .execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .unwrap();
    (dir, harness, separate)
}

async fn count(db: &sea_orm::DatabaseConnection, table: &str) -> i64 {
    db.query_one(Statement::from_string(
        DatabaseBackend::Sqlite,
        format!("SELECT COUNT(*) AS n FROM {table}"),
    ))
    .await
    .unwrap()
    .unwrap()
    .try_get("", "n")
    .unwrap()
}

fn upload_id(xml: &str) -> &str {
    xml.split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
}

#[tokio::test]
async fn signed_create_replays_original_snapshot_across_connections_and_rejects_changes() {
    let (_dir, harness, second) = setup().await;
    let query = &[("uploads", ""), ("decompress-zip", "out/")];
    let mut headers = controls("same-token");
    headers.insert(
        "x-amz-tagging",
        HeaderValue::from_static("ipfs-s3%3Azip-root=true&team=blue"),
    );
    headers.insert("content-type", HeaderValue::from_static("application/zip"));
    let first = create(&harness, query, headers.clone()).await;
    let first_status = first.status();
    let first_body = first.text().await.unwrap();
    assert_eq!(first_status, StatusCode::OK, "{first_body}");
    let original = upload_id(&first_body).to_owned();
    let record = store::multipart::v2_zip::read_by_upload(&second, &original)
        .await
        .unwrap()
        .unwrap();
    let opts: ZipV2Options = serde_json::from_str(&record.captured_options).unwrap();
    assert_eq!(opts.token, "same-token");
    assert_eq!(opts.root_override, Some(true));
    assert!(opts.root_enabled);
    assert!(!opts.publish_source);
    assert!(!record.rule_revision.is_empty());
    assert_eq!(record.owner, "test");
    assert_eq!(count(&second, "multipart_uploads").await, 1);
    assert_eq!(count(&second, "zip_v2_mpu_intakes").await, 1);
    assert_eq!(count(&second, "zip_v2_executions").await, 1);
    let complete = support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploadId", original.as_str())],
        br#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"part"</ETag></Part></CompleteMultipartUpload>"#.to_vec(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(complete.status(), StatusCode::BAD_REQUEST);
    assert_eq!(count(&second, "multipart_uploads").await, 1);
    assert_eq!(count(&second, "objects").await, 0);
    assert!(harness.captured_add_file_bytes().is_empty());
    let replay = create(&harness, query, headers.clone()).await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(upload_id(&replay.text().await.unwrap()), original);
    assert_eq!(count(&second, "multipart_uploads").await, 1);

    for changed in [
        {
            let mut h = headers.clone();
            h.insert(
                "x-amz-tagging",
                HeaderValue::from_static("ipfs-s3%3Azip-root=false&team=blue"),
            );
            h
        },
        {
            let mut h = headers.clone();
            h.insert("content-type", HeaderValue::from_static("text/plain"));
            h
        },
        {
            let mut h = headers.clone();
            h.insert("x-ipfs3-zip-targets", HeaderValue::from_static("none"));
            h
        },
    ] {
        let response = create(&harness, query, changed).await;
        assert_eq!(
            response.status(),
            StatusCode::CONFLICT,
            "{}",
            response.text().await.unwrap()
        );
    }
    let moved = create(
        &harness,
        &[("uploads", ""), ("decompress-zip", "other/")],
        headers.clone(),
    )
    .await;
    assert_eq!(moved.status(), StatusCode::CONFLICT);
    assert_eq!(count(&second, "multipart_uploads").await, 1);
    assert!(harness.captured_add_file_bytes().is_empty());

    // A completed/aborted upload is never resurrected by a matching Create replay.
    second
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "DELETE FROM multipart_uploads WHERE upload_id=?",
            vec![original.clone().into()],
        ))
        .await
        .unwrap();
    let stale = create(&harness, query, headers).await;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    let retained = store::multipart::v2_zip::read_by_upload(&second, &original)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.active_upload_id, None);
    assert_eq!(count(&second, "zip_v2_mpu_intakes").await, 1);
}

#[tokio::test]
async fn invalid_v2_never_creates_rows_and_plain_legacy_still_works() {
    let (_dir, harness, second) = setup().await;
    let query = &[("uploads", ""), ("decompress-zip", "out/")];
    for (q, headers) in [
        (query.as_slice(), {
            let mut h = controls("bad");
            h.insert("x-ipfs3-zip-token", HeaderValue::from_static("bad/token"));
            h
        }),
        (query.as_slice(), {
            let mut h = controls("bad");
            h.insert(
                "x-ipfs3-zip-publish-source",
                HeaderValue::from_static("False"),
            );
            h
        }),
        (query.as_slice(), {
            let mut h = controls("bad");
            h.insert("x-ipfs3-zip-unknown", HeaderValue::from_static("true"));
            h
        }),
        (
            &[
                ("uploads", ""),
                ("decompress-zip", "out/"),
                ("decompress-zip-result", "false"),
            ][..],
            controls("bad"),
        ),
        (query.as_slice(), {
            let mut h = controls("bad");
            h.insert(
                "x-amz-server-side-encryption",
                HeaderValue::from_static("AES256"),
            );
            h
        }),
    ] {
        assert_ne!(create(&harness, q, headers).await.status(), StatusCode::OK);
    }
    let no_zip = create(&harness, &[("uploads", "")], controls("bad")).await;
    assert_ne!(no_zip.status(), StatusCode::OK);
    assert_eq!(count(&second, "multipart_uploads").await, 0);
    assert_eq!(count(&second, "zip_v2_executions").await, 0);
    let legacy = create(&harness, query, HeaderMap::new()).await;
    assert_eq!(
        legacy.status(),
        StatusCode::OK,
        "{}",
        legacy.text().await.unwrap()
    );
    assert_eq!(count(&second, "multipart_uploads").await, 1);
    assert_eq!(count(&second, "zip_v2_mpu_intakes").await, 0);
    assert!(harness.captured_add_file_bytes().is_empty());
}
