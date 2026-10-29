use super::common::{
    SINGLE_ENTRY_BYTES, assert_latest_absent, assert_s3_error, assert_signed_body, kubo_log,
    listed_db_keys, scripted, signed_put, test_bucket,
};
use crate::support::decompress::{
    assert_no_kubo_calls, assert_pin_calls, latest_observed_request, legal_single_entry_zip,
    start_harness,
};
use crate::support::sigv4::{presign_sigv4_query, send_sigv4};
use chrono::Utc;
use http::{HeaderMap, StatusCode};
use ipfs_s3_gateway::store;
use sea_orm::EntityTrait;

#[tokio::test]
async fn test_sigv4_valid_request_reaches_decompress_route() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry"],
        vec![
            ("QmArchive", archive.clone()),
            ("QmEntry", SINGLE_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;

    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "")],
        archive.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("decompress response body");
    assert!(body.contains("<DecompressZipResult>"));
    store::object::get_latest(harness.state.store.db(), &harness.bucket, "archive.zip")
        .await
        .expect("archive DB row");
    store::object::get_latest(harness.state.store.db(), &harness.bucket, "file.txt")
        .await
        .expect("entry DB row");
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "file.txt", SINGLE_ENTRY_BYTES).await;
    assert_pin_calls(&harness, "/api/v0/pin/add", &["QmArchive", "QmEntry"], &[]).await;
}

#[tokio::test]
async fn test_sigv4_wrong_signature_is_rejected_before_kubo() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(&[], vec![])).await;
    let response = send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("decompress-zip", "")],
        archive,
        HeaderMap::new(),
        "wrong",
    )
    .await;
    assert_s3_error(response, StatusCode::FORBIDDEN, "SignatureDoesNotMatch", "").await;
    assert_latest_absent(&harness, "archive.zip").await;
    assert_no_kubo_calls(&harness).await;
}

#[tokio::test]
async fn test_sigv4_query_tuple_decodes_prefix_once() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry"],
        vec![("QmArchive", archive.clone())],
    ))
    .await;
    let query = [("decompress-zip", "prefix/nested/")];
    let response = send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &query,
        archive,
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .url()
            .as_str()
            .contains("decompress-zip=prefix%2Fnested%2F")
    );
    store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "prefix/nested/file.txt",
    )
    .await
    .expect("decoded target key");
    assert_latest_absent(&harness, "prefix%2Fnested%2Ffile.txt").await;
}

#[tokio::test]
async fn test_presigned_put_signs_custom_query_and_lists_gets_objects() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry"],
        vec![
            ("QmArchive", archive.clone()),
            ("QmEntry", SINGLE_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;
    let url = presign_sigv4_query(
        &reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[
            ("decompress-zip", "prefix/"),
            ("decompress-zip-result", "true"),
        ],
        "test",
        "test",
        None,
        900,
        Utc::now(),
    );
    let response = reqwest::Client::new()
        .put(url)
        .body(archive.clone())
        .send()
        .await
        .expect("presigned PUT");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("decompress result body");
    assert!(body.contains("<DecompressZipResult>"));
    let observed = latest_observed_request(&harness).await;
    assert!(
        observed
            .uri
            .query()
            .unwrap_or_default()
            .contains("X-Amz-Algorithm")
    );
    assert!(!observed.headers.contains_key(http::header::AUTHORIZATION));

    let pages = test_bucket(&harness)
        .list(String::new(), None)
        .await
        .expect("ListObjectsV2");
    let listed: Vec<_> = pages
        .iter()
        .flat_map(|page| page.contents.iter().map(|object| object.key.as_str()))
        .collect();
    assert!(listed.contains(&"archive.zip"));
    assert!(listed.contains(&"prefix/file.txt"));
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "prefix/file.txt", SINGLE_ENTRY_BYTES).await;
    assert_pin_calls(&harness, "/api/v0/pin/add", &["QmArchive", "QmEntry"], &[]).await;
    let log = kubo_log(&harness).await.join("\n");
    assert!(log.contains("/api/v0/add"));
    assert!(log.contains("/api/v0/pin/add"));
    assert!(log.contains("/api/v0/cat"));
    assert!(log.contains("QmArchive"));
    assert!(log.contains("QmEntry"));
}

#[tokio::test]
async fn test_presigned_space_target_raw_plus_rewrite_is_semantically_stable() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry"],
        vec![
            ("QmArchive", archive.clone()),
            ("QmEntry", SINGLE_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;
    let signed_url = presign_sigv4_query(
        &reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("decompress-zip", "reports Q3/")],
        "test",
        "test",
        None,
        900,
        Utc::now(),
    );
    assert!(signed_url.contains("decompress-zip=reports%20Q3%2F"));
    let rewritten_url = signed_url.replacen(
        "decompress-zip=reports%20Q3%2F",
        "decompress-zip=reports+Q3%2F",
        1,
    );
    assert_ne!(rewritten_url, signed_url);

    let response = reqwest::Client::new()
        .put(rewritten_url)
        .body(archive)
        .send()
        .await
        .expect("rewritten presigned PUT");

    assert_eq!(response.status(), StatusCode::OK);
    let observed = latest_observed_request(&harness).await;
    assert!(
        observed
            .uri
            .query()
            .unwrap_or_default()
            .contains("decompress-zip=reports+Q3%2F")
    );
    store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "reports Q3/file.txt",
    )
    .await
    .expect("space-decoded entry DB row");
    assert_latest_absent(&harness, "reports+Q3/file.txt").await;
    assert!(
        !listed_db_keys(&harness)
            .await
            .iter()
            .any(|key| key == "reports+Q3/file.txt")
    );
}

#[tokio::test]
async fn test_presigned_put_tampered_decompress_query_is_rejected_without_mutation() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(&[], vec![])).await;
    let url = presign_sigv4_query(
        &reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[
            ("decompress-zip", "prefix/"),
            ("decompress-zip-result", "true"),
        ],
        "test",
        "test",
        None,
        900,
        Utc::now(),
    );
    let response = reqwest::Client::new()
        .put(format!("{url}&decompress-zip=other%2F"))
        .body(archive)
        .send()
        .await
        .expect("tampered presigned PUT");
    assert_s3_error(response, StatusCode::FORBIDDEN, "SignatureDoesNotMatch", "").await;
    for key in ["archive.zip", "prefix/file.txt", "other/file.txt"] {
        assert_latest_absent(&harness, key).await;
    }
    assert!(
        ipfs_s3_gateway::store::entities::multipart_upload::Entity::find()
            .all(harness.state.store.db())
            .await
            .expect("multipart upload rows")
            .is_empty()
    );
    assert!(
        ipfs_s3_gateway::store::entities::multipart_part::Entity::find()
            .all(harness.state.store.db())
            .await
            .expect("multipart part rows")
            .is_empty()
    );
    assert_no_kubo_calls(&harness).await;
}
