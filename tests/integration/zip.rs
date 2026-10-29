use super::common::{
    FIRST_ENTRY_BYTES, SECOND_ENTRY_BYTES, assert_latest_absent, assert_s3_error,
    assert_signed_body, kubo_log, kubo_query_args, listed_db_keys, scripted, signed_put,
    sse_c_headers,
};
use crate::support::decompress::{
    AddReply, KuboScript, abort_multipart, archive_key_collision_zip, assert_no_kubo_calls,
    assert_pin_calls, complete_multipart, create_multipart, duplicate_entry_zip,
    latest_observed_request, legal_single_entry_zip, legal_two_entry_zip, start_harness,
    traversal_zip, upload_part,
};
use crate::support::sigv4::{send_sigv4, send_sigv4_chunked_http1};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::store;
use sea_orm::{ConnectionTrait, DatabaseBackend, EntityTrait, Statement};
use std::collections::HashMap;

const FIRST_DUPLICATE_BYTES: &[u8] = b"first duplicate bytes";

const SECOND_DUPLICATE_BYTES: &[u8] = b"second duplicate bytes";

#[tokio::test]
async fn test_put_decompress_zip_signed_default_result() {
    let archive = legal_two_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry1", "QmEntry2"],
        vec![
            ("QmArchive", archive.clone()),
            ("QmEntry1", FIRST_ENTRY_BYTES.to_vec()),
            ("QmEntry2", SECOND_ENTRY_BYTES.to_vec()),
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
    assert_eq!(response.headers()["etag"], "\"QmArchive\"");
    let body = response.text().await.expect("decompress result body");
    assert!(body.contains("<DecompressZipResult>"));
    assert!(body.contains("<ExtractedCount>2</ExtractedCount>"));
    for key in ["archive.zip", "first.txt", "second.txt"] {
        store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
            .await
            .unwrap_or_else(|error| panic!("latest {key}: {error}"));
    }
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "first.txt", FIRST_ENTRY_BYTES).await;
    assert_signed_body(&harness, "second.txt", SECOND_ENTRY_BYTES).await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmArchive", "QmEntry1", "QmEntry2"],
        &[],
    )
    .await;
}

#[tokio::test]
async fn test_put_duplicate_entry_key_last_wins() {
    let archive = duplicate_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmFirstDuplicate", "QmSecondDuplicate"],
        vec![
            ("QmArchive", archive.clone()),
            ("QmFirstDuplicate", FIRST_DUPLICATE_BYTES.to_vec()),
            ("QmSecondDuplicate", SECOND_DUPLICATE_BYTES.to_vec()),
        ],
    ))
    .await;
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "prefix/")],
        archive.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        store::object::get_latest(
            harness.state.store.db(),
            &harness.bucket,
            "prefix/duplicate.txt",
        )
        .await
        .expect("latest duplicate entry")
        .cid,
        "QmSecondDuplicate"
    );
    assert_signed_body(&harness, "prefix/duplicate.txt", SECOND_DUPLICATE_BYTES).await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmArchive", "QmFirstDuplicate", "QmSecondDuplicate"],
        &[],
    )
    .await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmArchive", "QmFirstDuplicate", "QmSecondDuplicate"],
    )
    .await;
}

#[tokio::test]
async fn test_put_decompress_zip_signed_result_false() {
    let archive = legal_two_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry1", "QmEntry2"],
        vec![("QmArchive", archive.clone())],
    ))
    .await;
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", ""), ("decompress-zip-result", "false")],
        archive,
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["etag"], "\"QmArchive\"");
    assert!(
        response
            .bytes()
            .await
            .expect("empty response body")
            .is_empty()
    );
    assert_eq!(
        listed_db_keys(&harness).await,
        vec!["archive.zip", "first.txt", "second.txt"]
    );
}

#[tokio::test]
async fn test_put_decompress_zip_traversal_hides_db_and_keeps_archive_pin() {
    let archive = traversal_zip();
    let harness = start_harness(scripted(
        &["QmArchive"],
        vec![("QmArchive", archive.clone())],
    ))
    .await;
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "prefix/")],
        archive,
        HeaderMap::new(),
    )
    .await;
    assert_s3_error(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidParameterValue",
        "",
    )
    .await;
    for key in ["archive.zip", "escape.txt", "prefix/escape.txt"] {
        assert_latest_absent(&harness, key).await;
    }
    assert!(listed_db_keys(&harness).await.is_empty());
    assert_pin_calls(&harness, "/api/v0/pin/add", &["QmArchive"], &[]).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmArchive"]).await;
}

#[tokio::test]
async fn test_put_decompress_zip_archive_key_collision_is_global_reject() {
    let archive = archive_key_collision_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmCollisionEntry"],
        vec![("QmArchive", archive.clone())],
    ))
    .await;
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "")],
        archive,
        HeaderMap::new(),
    )
    .await;
    assert_s3_error(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidParameterValue",
        "zip entry collides with archive key: archive.zip",
    )
    .await;
    assert_latest_absent(&harness, "archive.zip").await;
    assert!(listed_db_keys(&harness).await.is_empty());
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmArchive", "QmCollisionEntry"],
        &[],
    )
    .await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmArchive", "QmCollisionEntry"],
    )
    .await;
}

#[tokio::test]
async fn test_put_decompress_zip_one_entry_kubo_failure_is_partial() {
    let archive = legal_two_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![
            AddReply::Ok("QmArchive"),
            AddReply::Error(StatusCode::INTERNAL_SERVER_ERROR, "entry add failed"),
            AddReply::Ok("QmEntry2"),
        ],
        cat_bodies: HashMap::from([
            ("QmArchive".to_owned(), archive.clone()),
            ("QmEntry2".to_owned(), SECOND_ENTRY_BYTES.to_vec()),
        ]),
    })
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
    let body = response.text().await.expect("partial response body");
    assert!(body.contains("<FailedCount>1</FailedCount>"));
    assert!(body.contains("EntryUploadFailed"));
    store::object::get_latest(harness.state.store.db(), &harness.bucket, "archive.zip")
        .await
        .expect("archive latest row");
    assert_latest_absent(&harness, "first.txt").await;
    store::object::get_latest(harness.state.store.db(), &harness.bucket, "second.txt")
        .await
        .expect("second entry latest row");
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "second.txt", SECOND_ENTRY_BYTES).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmArchive", "QmEntry2"]).await;
}

#[tokio::test]
async fn versioning_direct_zip_publication_rollback_is_atomic_and_keeps_pins() {
    let archive = legal_two_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry1", "QmEntry2"],
        vec![
            ("QmArchive", archive.clone()),
            ("QmEntry2", SECOND_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;
    harness
        .state
        .store
        .db()
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "CREATE TRIGGER fail_first_entry BEFORE INSERT ON objects \
             WHEN NEW.key = 'first.txt' \
             BEGIN SELECT RAISE(FAIL, 'forced entry publish failure'); END;",
        ))
        .await
        .expect("install entry publish failure trigger");
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "")],
        archive.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = response.text().await.expect("S3 error response body");
    assert!(
        body.contains("<Code>InternalError</Code>"),
        "expected an S3 InternalError response"
    );
    assert!(
        !body.contains("forced entry publish failure"),
        "S3 database errors must not expose internal database details"
    );

    for key in ["archive.zip", "first.txt", "second.txt"] {
        assert_latest_absent(&harness, key).await;
    }
    let db = harness.state.store.db();
    assert!(
        store::entities::object::Entity::find()
            .all(db)
            .await
            .expect("load object rows after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no object rows"
    );
    assert!(
        store::entities::object_tag::Entity::find()
            .all(db)
            .await
            .expect("load object tags after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no object tags"
    );
    assert!(
        store::entities::pin_lease::Entity::find()
            .all(db)
            .await
            .expect("load pin leases after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no pin leases"
    );
    assert!(
        store::entities::pin_lease_target::Entity::find()
            .all(db)
            .await
            .expect("load pin targets after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no pin targets"
    );
    assert!(
        store::entities::remote_pin::Entity::find()
            .all(db)
            .await
            .expect("load remote pins after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no remote pins"
    );
    assert!(
        store::entities::pin_provider_usage::Entity::find()
            .all(db)
            .await
            .expect("load provider usage after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no provider usage"
    );
    assert!(
        store::entities::pin_job::Entity::find()
            .all(db)
            .await
            .expect("load pin jobs after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no pin jobs"
    );
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmArchive", "QmEntry1", "QmEntry2"],
    )
    .await;
}

#[tokio::test]
async fn test_put_decompress_zip_rejects_sse_s3() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "")],
        legal_single_entry_zip(),
        headers,
    )
    .await;
    assert_s3_error(response, StatusCode::BAD_REQUEST, "InvalidArgument", "").await;
    assert_no_kubo_calls(&harness).await;
}

#[tokio::test]
async fn test_put_decompress_zip_rejects_sse_c() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "")],
        legal_single_entry_zip(),
        sse_c_headers(),
    )
    .await;
    assert_s3_error(response, StatusCode::BAD_REQUEST, "InvalidArgument", "").await;
    assert_no_kubo_calls(&harness).await;
}

#[tokio::test]
async fn test_multipart_decompress_signed_default_result() {
    let archive = legal_two_entry_zip();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot", "QmEntry1", "QmEntry2"],
        vec![
            ("QmPart", archive.clone()),
            ("QmRoot", archive.clone()),
            ("QmEntry1", FIRST_ENTRY_BYTES.to_vec()),
            ("QmEntry2", SECOND_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;
    let upload_id =
        create_multipart(&harness, "archive.zip", &[("decompress-zip", "prefix/")]).await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive.clone()).await;
    let response = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("multipart decompress body");
    assert!(body.contains("<DecompressZipResult>"));
    for key in ["archive.zip", "prefix/first.txt", "prefix/second.txt"] {
        store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
            .await
            .unwrap_or_else(|error| panic!("latest {key}: {error}"));
    }
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("parts after complete")
            .is_empty()
    );
    assert_eq!(
        kubo_query_args(&harness, "/api/v0/cat").await,
        vec!["QmPart", "QmRoot"]
    );
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmPart", "QmRoot", "QmEntry1", "QmEntry2"],
        &[],
    )
    .await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmPart", "QmRoot", "QmEntry1", "QmEntry2"],
    )
    .await;
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "prefix/first.txt", FIRST_ENTRY_BYTES).await;
    assert_signed_body(&harness, "prefix/second.txt", SECOND_ENTRY_BYTES).await;
}

#[tokio::test]
async fn test_multipart_duplicate_entry_key_last_wins() {
    let archive = duplicate_entry_zip();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot", "QmFirstDuplicate", "QmSecondDuplicate"],
        vec![
            ("QmPart", archive.clone()),
            ("QmRoot", archive.clone()),
            ("QmFirstDuplicate", FIRST_DUPLICATE_BYTES.to_vec()),
            ("QmSecondDuplicate", SECOND_DUPLICATE_BYTES.to_vec()),
        ],
    ))
    .await;
    let upload_id =
        create_multipart(&harness, "archive.zip", &[("decompress-zip", "prefix/")]).await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let response = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        store::object::get_latest(
            harness.state.store.db(),
            &harness.bucket,
            "prefix/duplicate.txt",
        )
        .await
        .expect("latest duplicate entry")
        .cid,
        "QmSecondDuplicate"
    );
    assert_signed_body(&harness, "prefix/duplicate.txt", SECOND_DUPLICATE_BYTES).await;
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("parts after complete")
            .is_empty()
    );
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmPart", "QmRoot", "QmFirstDuplicate", "QmSecondDuplicate"],
        &[],
    )
    .await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmPart", "QmRoot", "QmFirstDuplicate", "QmSecondDuplicate"],
    )
    .await;
}

#[tokio::test]
async fn test_multipart_decompress_signed_result_false() {
    let archive = legal_two_entry_zip();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot", "QmEntry1", "QmEntry2"],
        vec![
            ("QmPart", archive.clone()),
            ("QmRoot", archive.clone()),
            ("QmEntry1", FIRST_ENTRY_BYTES.to_vec()),
            ("QmEntry2", SECOND_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;
    let upload_id = create_multipart(
        &harness,
        "archive.zip",
        &[
            ("decompress-zip", "prefix/"),
            ("decompress-zip-result", "false"),
        ],
    )
    .await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive.clone()).await;
    let response = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["etag"], "\"QmRoot\"");
    let body = response.text().await.expect("multipart response body");
    assert!(body.contains("<CompleteMultipartUploadResult>"));
    assert!(!body.contains("<DecompressZipResult>"));
    assert_eq!(
        listed_db_keys(&harness).await,
        vec!["archive.zip", "prefix/first.txt", "prefix/second.txt"]
    );
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("parts after complete")
            .is_empty()
    );
    assert_eq!(
        kubo_query_args(&harness, "/api/v0/cat").await,
        vec!["QmPart", "QmRoot"]
    );
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmPart", "QmRoot", "QmEntry1", "QmEntry2"],
        &[],
    )
    .await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmPart", "QmRoot", "QmEntry1", "QmEntry2"],
    )
    .await;
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "prefix/first.txt", FIRST_ENTRY_BYTES).await;
    assert_signed_body(&harness, "prefix/second.txt", SECOND_ENTRY_BYTES).await;
}

#[tokio::test]
async fn test_complete_xml_content_length_over_limit_rejected_without_complete_mutation() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(&["QmPart"], vec![("QmPart", archive.clone())])).await;
    let upload_id = create_multipart(&harness, "archive.zip", &[]).await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let upload_before = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("upload snapshot");
    let parts_before = store::multipart::list_parts(harness.state.store.db(), &upload_id)
        .await
        .expect("part snapshot");
    let kubo_before = kubo_log(&harness).await;
    let too_large_xml = vec![b'x'; 4 * 1024 * 1024 + 1];
    let response = send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploadId", upload_id.as_str())],
        too_large_xml,
        HeaderMap::new(),
        "test",
    )
    .await;
    let observed = latest_observed_request(&harness).await;
    let declared_length = observed
        .headers
        .get(http::header::CONTENT_LENGTH)
        .expect("declared Content-Length")
        .to_str()
        .expect("numeric Content-Length")
        .parse::<usize>()
        .expect("Content-Length number");
    assert!(declared_length > 4 * 1024 * 1024);
    assert_s3_error(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidRequest",
        "CompleteMultipartUpload XML exceeds 4 MiB",
    )
    .await;
    assert_eq!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .expect("unchanged upload"),
        upload_before
    );
    assert_eq!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("unchanged parts"),
        parts_before
    );
    assert_latest_absent(&harness, "archive.zip").await;
    assert_eq!(
        kubo_log(&harness).await,
        kubo_before,
        "no Complete Kubo calls"
    );
    assert_eq!(etag, "QmPart", "setup part ETag is retained");
}

#[tokio::test]
async fn test_complete_xml_chunked_over_limit_rejected_without_complete_mutation() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(&["QmPart"], vec![("QmPart", archive.clone())])).await;
    let upload_id = create_multipart(&harness, "archive.zip", &[]).await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let upload_before = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("upload snapshot");
    let parts_before = store::multipart::list_parts(harness.state.store.db(), &upload_id)
        .await
        .expect("part snapshot");
    let kubo_before = kubo_log(&harness).await;
    let too_large_xml = vec![b'x'; 4 * 1024 * 1024 + 1];
    let chunk_size = too_large_xml.len().div_ceil(3);
    let chunks = too_large_xml
        .chunks(chunk_size)
        .map(Bytes::copy_from_slice)
        .collect();
    let response = send_sigv4_chunked_http1(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploadId", upload_id.as_str())],
        chunks,
        HeaderMap::new(),
        "test",
    )
    .await;
    let observed = latest_observed_request(&harness).await;
    assert_eq!(observed.method, http::Method::POST);
    assert!(
        observed
            .headers
            .get(http::header::TRANSFER_ENCODING)
            .expect("Transfer-Encoding")
            .to_str()
            .expect("Transfer-Encoding value")
            .contains("chunked")
    );
    assert!(!observed.headers.contains_key(http::header::CONTENT_LENGTH));
    assert_eq!(
        observed.headers["x-amz-decoded-content-length"],
        (4 * 1024 * 1024 + 1).to_string()
    );
    assert_s3_error(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidRequest",
        "CompleteMultipartUpload XML exceeds 4 MiB",
    )
    .await;
    assert_eq!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .expect("unchanged upload"),
        upload_before
    );
    assert_eq!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("unchanged parts"),
        parts_before
    );
    assert_latest_absent(&harness, "archive.zip").await;
    assert_eq!(
        kubo_log(&harness).await,
        kubo_before,
        "no Complete Kubo calls"
    );
    assert_eq!(etag, "QmPart", "setup part ETag is retained");
}

#[tokio::test]
async fn test_multipart_traversal_keeps_root_pin_and_retry_state() {
    let archive = traversal_zip();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot"],
        vec![("QmPart", archive.clone()), ("QmRoot", archive.clone())],
    ))
    .await;
    let upload_id =
        create_multipart(&harness, "archive.zip", &[("decompress-zip", "prefix/")]).await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let response =
        complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag.clone())]).await;
    assert_s3_error(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidParameterValue",
        "",
    )
    .await;
    for key in ["archive.zip", "prefix/escape.txt"] {
        assert_latest_absent(&harness, key).await;
    }
    assert_eq!(
        store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
            .await
            .expect("retry part")
            .etag,
        etag
    );
    store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("retry upload");
    assert_pin_calls(&harness, "/api/v0/pin/add", &["QmRoot"], &[]).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmPart", "QmRoot"]).await;
}

#[tokio::test]
async fn test_multipart_archive_key_collision_is_global_reject_and_retryable() {
    let archive = archive_key_collision_zip();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot", "QmCollisionEntry"],
        vec![("QmPart", archive.clone()), ("QmRoot", archive.clone())],
    ))
    .await;
    let upload_id = create_multipart(&harness, "archive.zip", &[("decompress-zip", "")]).await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let response =
        complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag.clone())]).await;
    assert_s3_error(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidParameterValue",
        "zip entry collides with archive key: archive.zip",
    )
    .await;
    assert_latest_absent(&harness, "archive.zip").await;
    assert!(listed_db_keys(&harness).await.is_empty());
    store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("retryable upload row");
    assert_eq!(
        store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
            .await
            .expect("retryable part row")
            .etag,
        etag
    );
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmPart", "QmRoot", "QmCollisionEntry"],
        &[],
    )
    .await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmPart", "QmRoot", "QmCollisionEntry"],
    )
    .await;
}

#[tokio::test]
async fn test_multipart_abort_signed_removes_rows_and_keeps_part_pin() {
    let harness = start_harness(scripted(&["QmPart"], vec![])).await;
    let upload_id =
        create_multipart(&harness, "archive.zip", &[("decompress-zip", "prefix/")]).await;
    upload_part(
        &harness,
        "archive.zip",
        &upload_id,
        1,
        legal_single_entry_zip(),
    )
    .await;
    let response = abort_multipart(&harness, "archive.zip", &upload_id).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("removed parts")
            .is_empty()
    );
    assert_latest_absent(&harness, "archive.zip").await;
    assert_latest_absent(&harness, "prefix/file.txt").await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmPart"]).await;
    assert!(
        !kubo_log(&harness)
            .await
            .iter()
            .any(|entry| entry.contains("/api/v0/cat")),
        "abort must not cat content"
    );
}

#[tokio::test]
async fn test_multipart_single_part_equal_root_remains_readable() {
    let content = b"standard multipart bytes".to_vec();
    let harness = start_harness(scripted(
        &["QmPart", "QmPart"],
        vec![("QmPart", content.clone())],
    ))
    .await;
    let upload_id = create_multipart(&harness, "archive.bin", &[]).await;
    let etag = upload_part(&harness, "archive.bin", &upload_id, 1, content.clone()).await;
    let response = complete_multipart(&harness, "archive.bin", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("complete response body");
    assert!(body.contains("<CompleteMultipartUploadResult>"));
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "archive.bin")
            .await
            .expect("completed latest row")
            .cid,
        "QmPart"
    );
    assert_signed_body(&harness, "archive.bin", &content).await;
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("removed parts")
            .is_empty()
    );
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmPart"]).await;
}

#[tokio::test]
async fn test_multipart_shared_part_cid_survives_replace_abort_and_complete() {
    let shared = b"shared bytes".to_vec();
    let harness = start_harness(scripted(
        &[
            "QmSharedPart",
            "QmSharedPart",
            "QmReplacement",
            "QmSharedPart",
            "QmSharedPart",
            "QmRoot",
        ],
        vec![
            ("QmSharedPart", shared.clone()),
            ("QmReplacement", b"replacement bytes".to_vec()),
            ("QmRoot", shared.clone()),
        ],
    ))
    .await;
    let put = signed_put(
        &harness,
        "shared.bin",
        &[],
        shared.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);

    let replace_upload = create_multipart(&harness, "replace.bin", &[]).await;
    upload_part(&harness, "replace.bin", &replace_upload, 1, shared.clone()).await;
    upload_part(
        &harness,
        "replace.bin",
        &replace_upload,
        1,
        b"replacement bytes".to_vec(),
    )
    .await;
    assert_signed_body(&harness, "shared.bin", &shared).await;

    let abort_upload = create_multipart(&harness, "abort.bin", &[]).await;
    upload_part(&harness, "abort.bin", &abort_upload, 1, shared.clone()).await;
    let abort = abort_multipart(&harness, "abort.bin", &abort_upload).await;
    assert_eq!(abort.status(), StatusCode::NO_CONTENT);
    assert_signed_body(&harness, "shared.bin", &shared).await;

    let complete_upload = create_multipart(&harness, "complete.bin", &[]).await;
    let etag = upload_part(
        &harness,
        "complete.bin",
        &complete_upload,
        1,
        shared.clone(),
    )
    .await;
    let complete =
        complete_multipart(&harness, "complete.bin", &complete_upload, &[(1, etag)]).await;
    assert_eq!(complete.status(), StatusCode::OK);
    assert_signed_body(&harness, "shared.bin", &shared).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmSharedPart"]).await;
}

#[tokio::test]
async fn test_upload_part_db_failure_keeps_new_pin_and_old_record() {
    let harness = start_harness(scripted(&["QmOldPart", "QmNewPart"], vec![])).await;
    let upload_id = create_multipart(&harness, "archive.zip", &[]).await;
    upload_part(
        &harness,
        "archive.zip",
        &upload_id,
        1,
        b"old part bytes".to_vec(),
    )
    .await;
    harness
        .state
        .store
        .db()
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "CREATE TRIGGER fail_part_update BEFORE UPDATE ON multipart_parts \
             BEGIN SELECT RAISE(FAIL, 'forced part update failure'); END;",
        ))
        .await
        .expect("install part update failure trigger");
    let response = send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("partNumber", "1"), ("uploadId", upload_id.as_str())],
        b"new part bytes".to_vec(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = response.text().await.expect("failed upload-part body");
    assert!(body.contains("InternalError"));
    assert_eq!(
        store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
            .await
            .expect("original part row")
            .cid,
        "QmOldPart"
    );
    assert_pin_calls(&harness, "/api/v0/pin/add", &["QmNewPart"], &[]).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmOldPart", "QmNewPart"]).await;
}
