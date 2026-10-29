use super::common::{
    OwnedTestEndpoint, assert_import_state, assert_latest_absent, assert_put_cid_headers,
    assert_s3_error, assert_signed_body, install_unversioned_content_version, kubo_query_args,
    scripted, seed_running_import, signed_copy, signed_get, signed_get_with_headers, signed_head,
    signed_head_with_headers, signed_put, sse_c_headers, sse_c_headers_for, standard_script,
};
use crate::support::decompress::{
    AddReply, KuboBlockTarget, KuboScript, TestHarness, abort_multipart, assert_pin_calls,
    complete_multipart, complete_multipart_with_headers, complete_multipart_xml, create_multipart,
    create_multipart_with_headers, start_blocking_harness, start_harness, upload_part,
    upload_part_with_headers,
};
use crate::support::sigv4::presign_sigv4_query;
use base64::Engine as _;
use chrono::Utc;
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::store;
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use std::collections::HashMap;

fn copy_source_sse_c_headers_for(key: [u8; 32]) -> HeaderMap {
    let key_b64 = base64::engine::general_purpose::STANDARD.encode(key);
    let md5_b64 = base64::engine::general_purpose::STANDARD.encode(md5::compute(key).0);
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-copy-source-server-side-encryption-customer-algorithm",
        HeaderValue::from_static("AES256"),
    );
    headers.insert(
        "x-amz-copy-source-server-side-encryption-customer-key",
        HeaderValue::from_str(&key_b64).expect("base64 copy-source key header"),
    );
    headers.insert(
        "x-amz-copy-source-server-side-encryption-customer-key-md5",
        HeaderValue::from_str(&md5_b64).expect("base64 copy-source key MD5 header"),
    );
    headers
}

fn fixed_sse_c_ciphertext(key: [u8; 32], nonce: [u8; 12], plaintext: &[u8]) -> Vec<u8> {
    ipfs_s3_gateway::crypto::aes_gcm::encrypt_chunk(
        &ipfs_s3_gateway::crypto::ObjectKey { bytes: key },
        &nonce,
        plaintext,
    )
    .expect("fixed SSE-C ciphertext")
    .to_vec()
}

async fn seed_sse_c_object(
    harness: &TestHarness,
    key: &str,
    cid: &str,
    plaintext: &[u8],
    fingerprinted: bool,
    recorded_size: i64,
) {
    harness.set_cat_body(cid, fixed_sse_c_ciphertext([7; 32], [0x5a; 12], plaintext));
    let object_key = ipfs_s3_gateway::crypto::ObjectKey { bytes: [7; 32] };
    let fingerprint =
        fingerprinted.then(|| harness.state.master_key.sse_c_key_fingerprint(&object_key));
    store::object::upsert(
        harness.state.store.db(),
        &format!("id-{}", key.replace('/', "-")),
        &harness.bucket,
        key,
        cid,
        recorded_size,
        Some("application/octet-stream"),
        cid,
        None,
        true,
        None,
        fingerprint.as_deref(),
        false,
    )
    .await
    .expect("seed SSE-C object");
    install_unversioned_content_version(harness, key, Utc::now()).await;
}

fn inner_complete_request(
    harness: &TestHarness,
    key: &str,
    upload_id: &str,
    etag: &str,
    headers: HeaderMap,
) -> s3s::S3Request<s3s::dto::CompleteMultipartUploadInput> {
    s3s::S3Request {
        input: s3s::dto::CompleteMultipartUploadInput {
            bucket: harness.bucket.clone(),
            key: key.to_owned(),
            upload_id: upload_id.to_owned(),
            multipart_upload: Some(s3s::dto::CompletedMultipartUpload {
                parts: Some(vec![s3s::dto::CompletedPart {
                    e_tag: Some(s3s::dto::ETag::Strong(etag.to_owned())),
                    part_number: Some(1),
                    ..Default::default()
                }]),
            }),
            ..Default::default()
        },
        method: http::Method::POST,
        uri: format!("/{}/{key}?uploadId={upload_id}", harness.bucket)
            .parse()
            .unwrap(),
        headers,
        extensions: http::Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

async fn kubo_call_counts(harness: &TestHarness) -> (usize, usize, usize, usize) {
    let requests = harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log");
    let count = |path| {
        requests
            .iter()
            .filter(|request| request.url.path() == path)
            .count()
    };
    (
        count("/api/v0/add"),
        count("/api/v0/cat"),
        count("/api/v0/pin/add"),
        count("/api/v0/pin/rm"),
    )
}

#[tokio::test]
async fn mismatched_sse_c_upload_part_is_rejected_before_kubo_and_preserves_part() {
    let harness = start_harness(scripted(&["QmOriginalPart"], vec![])).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "customer-encrypted.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;
    let initial = signed_put(
        &harness,
        "customer-encrypted.bin",
        &[("partNumber", "1"), ("uploadId", upload_id.as_str())],
        b"original encrypted part".to_vec(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(initial.status(), StatusCode::OK);
    let calls_before = kubo_call_counts(&harness).await;
    let part_before = store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
        .await
        .expect("original part row");

    let mut missing_algorithm = sse_c_headers_for([7; 32]);
    missing_algorithm.remove("x-amz-server-side-encryption-customer-algorithm");
    let mut mixed_sse = sse_c_headers_for([7; 32]);
    mixed_sse.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );

    for headers in [sse_c_headers_for([8; 32]), missing_algorithm, mixed_sse] {
        let response = signed_put(
            &harness,
            "customer-encrypted.bin",
            &[("partNumber", "1"), ("uploadId", upload_id.as_str())],
            b"rejected replacement".to_vec(),
            headers,
        )
        .await;
        assert_s3_error(
            response,
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "SSE-C",
        )
        .await;
        assert_eq!(
            kubo_call_counts(&harness).await,
            calls_before,
            "rejected UploadPart must not call Kubo"
        );
        assert_eq!(
            store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
                .await
                .expect("unchanged part row"),
            part_before,
            "rejected UploadPart must retain the original part row"
        );
    }
}

#[tokio::test]
async fn legacy_sse_c_upload_part_claims_fingerprint_before_add() {
    let harness = start_harness(scripted(&["QmLegacyPart"], vec![])).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "legacy-customer-encrypted.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;
    harness
        .state
        .store
        .db()
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            format!(
                "UPDATE multipart_uploads SET sse_c_key_fingerprint = NULL WHERE upload_id = '{upload_id}'"
            ),
        ))
        .await
        .expect("clear legacy fingerprint");
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .expect("legacy upload")
            .sse_c_key_fingerprint
            .is_none()
    );

    let response = signed_put(
        &harness,
        "legacy-customer-encrypted.bin",
        &[("partNumber", "1"), ("uploadId", upload_id.as_str())],
        b"legacy encrypted part".to_vec(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let upload = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("claimed upload");
    let expected = harness
        .state
        .master_key
        .sse_c_key_fingerprint(&ipfs_s3_gateway::crypto::ObjectKey { bytes: [7; 32] });
    assert_eq!(
        upload.sse_c_key_fingerprint.as_deref(),
        Some(expected.as_str())
    );
    assert_eq!(kubo_call_counts(&harness).await, (1, 0, 1, 0));
}

#[tokio::test]
async fn mismatched_sse_c_complete_is_pre_kubo_and_upload_remains_retryable() {
    let harness = start_harness(scripted(&["QmEncryptedPart", "QmRoot"], vec![])).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "customer-encrypted.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;
    let part = signed_put(
        &harness,
        "customer-encrypted.bin",
        &[("partNumber", "1"), ("uploadId", upload_id.as_str())],
        b"encrypted multipart part".to_vec(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(part.status(), StatusCode::OK);
    let etag = part
        .headers()
        .get(http::header::ETAG)
        .expect("UploadPart ETag")
        .to_str()
        .expect("UploadPart ETag text")
        .trim_matches('"')
        .to_owned();
    let ciphertext = harness
        .captured_add_file_bytes()
        .into_iter()
        .next()
        .expect("captured encrypted part");
    harness.set_cat_body("QmEncryptedPart", ciphertext);

    let calls_before = kubo_call_counts(&harness).await;
    let upload_before = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("upload before rejected completes");
    let part_before = store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
        .await
        .expect("part before rejected completes");

    let mut missing_algorithm = sse_c_headers_for([7; 32]);
    missing_algorithm.remove("x-amz-server-side-encryption-customer-algorithm");
    let mut mixed_sse = sse_c_headers_for([7; 32]);
    mixed_sse.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );

    for headers in [sse_c_headers_for([8; 32]), missing_algorithm, mixed_sse] {
        let response = complete_multipart_with_headers(
            &harness,
            "customer-encrypted.bin",
            &upload_id,
            &[(1, etag.clone())],
            headers,
        )
        .await;
        assert_s3_error(
            response,
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "SSE-C",
        )
        .await;
        assert_eq!(
            kubo_call_counts(&harness).await,
            calls_before,
            "rejected CompleteMultipartUpload must not call Kubo"
        );
        assert_eq!(
            store::multipart::get_upload(harness.state.store.db(), &upload_id)
                .await
                .expect("unchanged upload row"),
            upload_before,
            "rejected CompleteMultipartUpload must retain the upload row"
        );
        assert_eq!(
            store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
                .await
                .expect("unchanged part row"),
            part_before,
            "rejected CompleteMultipartUpload must retain the part row"
        );
    }

    let response = complete_multipart_with_headers(
        &harness,
        "customer-encrypted.bin",
        &upload_id,
        &[(1, etag)],
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn legacy_sse_c_complete_claims_before_corrupt_ciphertext_error() {
    let harness = start_harness(scripted(&["QmEncryptedPart", "QmRoot"], vec![])).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "legacy-customer-encrypted.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;
    let part = signed_put(
        &harness,
        "legacy-customer-encrypted.bin",
        &[("partNumber", "1"), ("uploadId", upload_id.as_str())],
        b"legacy encrypted multipart part".to_vec(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(part.status(), StatusCode::OK);
    let etag = part
        .headers()
        .get(http::header::ETAG)
        .expect("UploadPart ETag")
        .to_str()
        .expect("UploadPart ETag text")
        .trim_matches('"')
        .to_owned();
    harness
        .state
        .store
        .db()
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            format!(
                "UPDATE multipart_uploads SET sse_c_key_fingerprint = NULL WHERE upload_id = '{upload_id}'"
            ),
        ))
        .await
        .expect("clear legacy fingerprint");
    harness.set_cat_body("QmEncryptedPart", b"corrupt ciphertext".to_vec());
    let calls_before = kubo_call_counts(&harness).await;

    let response = complete_multipart_with_headers(
        &harness,
        "legacy-customer-encrypted.bin",
        &upload_id,
        &[(1, etag.clone())],
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_s3_error(response, StatusCode::BAD_REQUEST, "InvalidPart", "decrypt").await;
    assert_eq!(
        kubo_call_counts(&harness).await,
        (
            calls_before.0,
            calls_before.1 + 1,
            calls_before.2,
            calls_before.3
        ),
        "corrupt ciphertext must cat once without root add, pin, or unpin"
    );

    let upload = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("retryable legacy upload");
    let expected = harness
        .state
        .master_key
        .sse_c_key_fingerprint(&ipfs_s3_gateway::crypto::ObjectKey { bytes: [7; 32] });
    assert_eq!(
        upload.sse_c_key_fingerprint.as_deref(),
        Some(expected.as_str())
    );
    assert_eq!(
        store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
            .await
            .expect("retryable legacy part")
            .etag,
        etag
    );
}

#[tokio::test]
async fn sse_c_abort_requires_no_customer_key() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "customer-encrypted.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;

    let response = abort_multipart(&harness, "customer-encrypted.bin", &upload_id).await;
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
    assert_eq!(kubo_call_counts(&harness).await, (0, 0, 0, 0));
}

#[tokio::test]
async fn test_standard_put_sse_s3_still_succeeds() {
    let harness = start_harness(standard_script(1)).await;
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );
    let response = signed_put(
        &harness,
        "encrypted.bin",
        &[],
        b"encrypted bytes".to_vec(),
        headers,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-amz-server-side-encryption"], "AES256");
    let latest =
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "encrypted.bin")
            .await
            .expect("encrypted latest row");
    assert!(latest.encrypted);
    assert!(latest.key_wrap.is_some());
}

#[tokio::test]
async fn test_standard_put_sse_c_still_succeeds() {
    let harness = start_harness(standard_script(1)).await;
    let response = signed_put(
        &harness,
        "customer-encrypted.bin",
        &[],
        b"customer encrypted bytes".to_vec(),
        sse_c_headers(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let latest = store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "customer-encrypted.bin",
    )
    .await
    .expect("customer encrypted latest row");
    assert!(latest.encrypted);
    assert!(latest.key_wrap.is_none());
}

#[tokio::test]
async fn test_standard_multipart_signed_still_succeeds() {
    let completed = b"standard multipart bytes".to_vec();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot"],
        vec![("QmPart", completed.clone()), ("QmRoot", completed.clone())],
    ))
    .await;
    let upload_id = create_multipart(&harness, "multipart.bin", &[]).await;
    let etag = upload_part(&harness, "multipart.bin", &upload_id, 1, completed.clone()).await;
    let response = complete_multipart(&harness, "multipart.bin", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("complete response body");
    assert!(body.contains("<CompleteMultipartUploadResult>"));
    let latest =
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "multipart.bin")
            .await
            .expect("completed multipart latest row");
    assert_eq!(latest.cid, "QmRoot");
    assert!(latest.multipart);
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("removed multipart parts")
            .is_empty()
    );
    assert_eq!(
        kubo_query_args(&harness, "/api/v0/cat").await,
        vec!["QmPart"]
    );
    assert_pin_calls(&harness, "/api/v0/pin/add", &["QmPart", "QmRoot"], &[]).await;
    assert_signed_body(&harness, "multipart.bin", &completed).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmPart", "QmRoot"]).await;
}

#[tokio::test]
async fn test_standard_multipart_complete_accepts_weak_part_etag_and_checksums() {
    let completed = b"standard multipart bytes".to_vec();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot"],
        vec![("QmPart", completed.clone()), ("QmRoot", completed.clone())],
    ))
    .await;
    let upload_id = create_multipart(&harness, "multipart-checksums.bin", &[]).await;
    let etag = upload_part(
        &harness,
        "multipart-checksums.bin",
        &upload_id,
        1,
        completed.clone(),
    )
    .await;
    let weak_etag = format!("W/\"{etag}\"");
    let xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag><ChecksumCRC32>crc32-value</ChecksumCRC32><ChecksumCRC32C>crc32c-value</ChecksumCRC32C><ChecksumCRC64NVME>crc64nvme-value</ChecksumCRC64NVME><ChecksumSHA1>sha1-value</ChecksumSHA1><ChecksumSHA256>sha256-value</ChecksumSHA256></Part></CompleteMultipartUpload>",
        quick_xml::escape::escape(&weak_etag),
    );
    assert!(xml.contains(&format!("W/&quot;{etag}&quot;")));
    let response =
        complete_multipart_xml(&harness, "multipart-checksums.bin", &upload_id, xml).await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("complete response body");
    assert!(body.contains("<CompleteMultipartUploadResult>"));
    let latest = store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "multipart-checksums.bin",
    )
    .await
    .expect("completed multipart latest row");
    assert_eq!(latest.cid, "QmRoot");
    assert!(latest.multipart);
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("removed multipart parts")
            .is_empty()
    );
    assert_eq!(
        kubo_query_args(&harness, "/api/v0/cat").await,
        vec!["QmPart"]
    );
    assert_signed_body(&harness, "multipart-checksums.bin", &completed).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmPart", "QmRoot"]).await;
}

#[tokio::test]
async fn test_standard_presigned_put_get_and_tamper_contract() {
    let cid = "QmStandardPresignedCid";
    let key = "standard-presigned.bin";
    let tampered_key = "tampered-presigned.bin";
    let payload = b"standard presigned exact payload".to_vec();
    let harness = start_harness(scripted(&[cid], vec![])).await;
    let client = reqwest::Client::new();
    let put_url = presign_sigv4_query(
        &reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        key,
        &[],
        "test",
        "test",
        None,
        900,
        Utc::now(),
    );

    let put = client
        .put(&put_url)
        .body(payload.clone())
        .send()
        .await
        .expect("presigned standard PUT");
    assert_eq!(put.status(), StatusCode::OK);
    assert_eq!(
        put.headers()[http::header::ETAG],
        "\"QmStandardPresignedCid\""
    );
    assert_eq!(put.headers()["x-amz-meta-ipfs-cid"], cid);
    assert_eq!(
        put.headers()["x-amz-meta-ipfs-url"],
        "ipfs://QmStandardPresignedCid"
    );
    assert_eq!(harness.captured_add_file_bytes(), vec![payload.clone()]);

    harness.set_cat_body(cid, payload.clone());
    let get_url = presign_sigv4_query(
        &reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        key,
        &[],
        "test",
        "test",
        None,
        900,
        Utc::now(),
    );
    let get = client
        .get(get_url)
        .send()
        .await
        .expect("presigned standard GET");
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(
        get.bytes().await.expect("presigned GET body").as_ref(),
        payload
    );

    let calls_before_tampering = kubo_call_counts(&harness).await;
    let tampered_path_url =
        put_url.replacen("/standard-presigned.bin?", "/tampered-presigned.bin?", 1);
    let tampered_path = client
        .put(tampered_path_url)
        .body(b"tampered path payload".to_vec())
        .send()
        .await
        .expect("tampered presigned path PUT");
    assert_s3_error(
        tampered_path,
        StatusCode::FORBIDDEN,
        "SignatureDoesNotMatch",
        "",
    )
    .await;
    assert_eq!(kubo_call_counts(&harness).await, calls_before_tampering);
    assert_latest_absent(&harness, tampered_key).await;

    let (signed_url_prefix, signature) = put_url
        .rsplit_once("X-Amz-Signature=")
        .expect("presigned URL includes signature");
    let replacement = if signature.starts_with('0') { '1' } else { '0' };
    let tampered_signature_url = format!(
        "{signed_url_prefix}X-Amz-Signature={replacement}{}",
        &signature[1..]
    );
    let tampered_signature = client
        .put(tampered_signature_url)
        .body(b"tampered signature payload".to_vec())
        .send()
        .await
        .expect("tampered presigned signature PUT");
    assert_s3_error(
        tampered_signature,
        StatusCode::FORBIDDEN,
        "SignatureDoesNotMatch",
        "",
    )
    .await;
    assert_eq!(kubo_call_counts(&harness).await, calls_before_tampering);
    assert_latest_absent(&harness, tampered_key).await;
}

#[tokio::test]
async fn put_object_cid_headers_absent_on_failure() {
    let harness = start_harness(KuboScript {
        add_replies: vec![AddReply::Error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "forced add failure",
        )],
        cat_bodies: HashMap::new(),
    })
    .await;

    let response = signed_put(
        &harness,
        "failed-cid-header.bin",
        &[],
        b"failed put payload".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(response.headers().get("x-amz-meta-ipfs-cid").is_none());
    assert!(response.headers().get("x-amz-meta-ipfs-url").is_none());
    assert_latest_absent(&harness, "failed-cid-header.bin").await;
    assert_eq!(kubo_call_counts(&harness).await, (1, 0, 0, 0));
}

#[tokio::test]
async fn v03_plaintext_get_and_head_range_matrix() {
    let cid = "QmV03PlaintextRange";
    let plaintext = b"0123456789".to_vec();
    let harness = start_harness(scripted(&[cid], vec![])).await;

    let put = signed_put(
        &harness,
        "plaintext-range.bin",
        &[],
        plaintext.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    harness.set_cat_body(cid, plaintext.clone());

    let mut range_headers = HeaderMap::new();
    range_headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=2-5"));
    let ranged = signed_get_with_headers(&harness, "plaintext-range.bin", range_headers).await;
    assert_eq!(ranged.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(ranged.headers()[http::header::CONTENT_LENGTH], "4");
    assert_eq!(
        ranged.headers()[http::header::CONTENT_RANGE],
        "bytes 2-5/10"
    );
    assert_eq!(
        ranged.bytes().await.expect("ranged GET body").as_ref(),
        b"2345"
    );

    let cat_requests = harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log")
        .into_iter()
        .filter(|request| request.url.path() == "/api/v0/cat")
        .collect::<Vec<_>>();
    assert_eq!(cat_requests.len(), 1, "ranged plaintext GET cats once");
    assert_eq!(
        cat_requests[0]
            .url
            .query_pairs()
            .map(|(name, value)| (name.into_owned(), value.into_owned()))
            .collect::<Vec<_>>(),
        vec![
            ("arg".to_owned(), cid.to_owned()),
            ("offset".to_owned(), "2".to_owned()),
            ("length".to_owned(), "4".to_owned()),
        ]
    );

    let calls_before_unsatisfiable = kubo_call_counts(&harness).await;
    let mut unsatisfiable_headers = HeaderMap::new();
    unsatisfiable_headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=10-12"));
    let unsatisfiable =
        signed_get_with_headers(&harness, "plaintext-range.bin", unsatisfiable_headers).await;
    assert_s3_error(
        unsatisfiable,
        StatusCode::RANGE_NOT_SATISFIABLE,
        "InvalidRange",
        "",
    )
    .await;
    assert_eq!(
        kubo_call_counts(&harness).await,
        calls_before_unsatisfiable,
        "unsatisfiable range must not call Kubo"
    );

    let head = signed_head(&harness, "plaintext-range.bin", Some("bytes=2-5")).await;
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(head.headers()[http::header::CONTENT_LENGTH], "4");
    assert!(head.headers().get(http::header::CONTENT_RANGE).is_none());
    assert!(head.bytes().await.expect("ranged HEAD body").is_empty());
}

#[tokio::test]
async fn v03_sse_s3_put_get_and_range_matrix() {
    let cid = "QmV03SseS3";
    let plaintext = b"0123456789abcdef".to_vec();
    let harness = start_harness(scripted(&[cid], vec![])).await;
    let mut encryption_headers = HeaderMap::new();
    encryption_headers.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );

    let put = signed_put(
        &harness,
        "sse-s3-range.bin",
        &[],
        plaintext.clone(),
        encryption_headers,
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_cid_headers(&put, cid);
    assert_eq!(put.headers()["x-amz-server-side-encryption"], "AES256");
    let ciphertext = harness
        .captured_add_file_bytes()
        .into_iter()
        .next()
        .expect("captured SSE-S3 ciphertext");
    assert_ne!(
        ciphertext, plaintext,
        "SSE-S3 Kubo add must receive ciphertext"
    );
    harness.set_cat_body(cid, ciphertext);

    let full = signed_get(&harness, "sse-s3-range.bin").await;
    assert_eq!(full.status(), StatusCode::OK);
    assert_eq!(
        full.bytes().await.expect("full SSE-S3 GET body").as_ref(),
        plaintext
    );

    let mut range_headers = HeaderMap::new();
    range_headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=4-9"));
    let ranged = signed_get_with_headers(&harness, "sse-s3-range.bin", range_headers).await;
    assert_eq!(ranged.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(ranged.headers()[http::header::CONTENT_LENGTH], "6");
    assert_eq!(
        ranged.headers()[http::header::CONTENT_RANGE],
        "bytes 4-9/16"
    );
    assert_eq!(
        ranged
            .bytes()
            .await
            .expect("ranged SSE-S3 GET body")
            .as_ref(),
        b"456789"
    );

    let cat_requests = harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log")
        .into_iter()
        .filter(|request| request.url.path() == "/api/v0/cat")
        .collect::<Vec<_>>();
    assert_eq!(
        cat_requests.len(),
        2,
        "full and ranged SSE-S3 GET cat once each"
    );
    for request in cat_requests {
        assert_eq!(
            request
                .url
                .query_pairs()
                .find(|(name, _)| name == "arg")
                .map(|(_, value)| value.into_owned())
                .as_deref(),
            Some(cid)
        );
        assert!(
            request
                .url
                .query_pairs()
                .all(|(name, _)| !matches!(name.as_ref(), "bytes" | "offset" | "length")),
            "encrypted GET must fully cat, decrypt, then slice"
        );
    }
}

#[tokio::test]
async fn v03_sse_c_put_get_and_wrong_key_range_matrix() {
    let cid = "QmV03SseC";
    let plaintext = b"0123456789abcdef".to_vec();
    let harness = start_harness(scripted(&[cid], vec![])).await;

    let put = signed_put(
        &harness,
        "sse-c-range.bin",
        &[],
        plaintext.clone(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_cid_headers(&put, cid);
    let ciphertext = harness
        .captured_add_file_bytes()
        .into_iter()
        .next()
        .expect("captured SSE-C ciphertext");
    assert_ne!(
        ciphertext, plaintext,
        "SSE-C Kubo add must receive ciphertext"
    );
    harness.set_cat_body(cid, ciphertext);

    let latest =
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "sse-c-range.bin")
            .await
            .expect("SSE-C latest row");
    assert!(latest.encrypted);
    assert!(latest.key_wrap.is_none());

    let full =
        signed_get_with_headers(&harness, "sse-c-range.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(full.status(), StatusCode::OK);
    assert_eq!(
        full.bytes().await.expect("full SSE-C GET body").as_ref(),
        plaintext
    );

    let mut correct_range_headers = sse_c_headers_for([7; 32]);
    correct_range_headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=3-8"));
    let ranged = signed_get_with_headers(&harness, "sse-c-range.bin", correct_range_headers).await;
    assert_eq!(ranged.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(ranged.headers()[http::header::CONTENT_LENGTH], "6");
    assert_eq!(
        ranged.headers()[http::header::CONTENT_RANGE],
        "bytes 3-8/16"
    );
    assert_eq!(
        ranged
            .bytes()
            .await
            .expect("ranged SSE-C GET body")
            .as_ref(),
        b"345678"
    );

    let mut wrong_range_headers = sse_c_headers_for([8; 32]);
    wrong_range_headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=3-8"));
    let wrong_key = signed_get_with_headers(&harness, "sse-c-range.bin", wrong_range_headers).await;
    assert_eq!(wrong_key.status(), StatusCode::FORBIDDEN);
    let wrong_body = wrong_key.text().await.expect("wrong-key SSE-C error body");
    assert!(
        wrong_body.contains("AccessDenied"),
        "wrong-key SSE-C response: {wrong_body}"
    );
    assert!(
        !wrong_body.contains(std::str::from_utf8(&plaintext).expect("plaintext is UTF-8")),
        "wrong-key SSE-C error must not leak plaintext: {wrong_body}"
    );
}

#[tokio::test]
async fn v03_sse_c_multipart_round_trip_matrix() {
    let part_cid = "QmV03SseCPart";
    let root_cid = "QmV03SseCRoot";
    let plaintext = b"SSE-C multipart plaintext".to_vec();
    let harness = start_harness(scripted(&[part_cid, root_cid], vec![])).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "sse-c-multipart.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;

    let upload = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("SSE-C multipart upload row");
    let fingerprint = upload
        .sse_c_key_fingerprint
        .as_deref()
        .expect("versioned SSE-C key fingerprint");
    assert!(fingerprint.starts_with("v1:hmac-sha256:"));
    assert_eq!(fingerprint.len(), "v1:hmac-sha256:".len() + 64);
    assert!(
        !fingerprint.contains(&base64::engine::general_purpose::STANDARD.encode([7; 32])),
        "SSE-C fingerprint must not persist the raw customer key"
    );
    assert!(
        !fingerprint
            .contains(&base64::engine::general_purpose::STANDARD.encode(md5::compute([7; 32]).0)),
        "SSE-C fingerprint must not persist the customer-key MD5"
    );

    let part_etag = upload_part_with_headers(
        &harness,
        "sse-c-multipart.bin",
        &upload_id,
        1,
        plaintext.clone(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    let part_ciphertext = harness
        .captured_add_file_bytes()
        .into_iter()
        .next()
        .expect("captured encrypted multipart part");
    assert_ne!(
        part_ciphertext, plaintext,
        "SSE-C multipart part Kubo add must receive ciphertext"
    );
    harness.set_cat_body(part_cid, part_ciphertext);

    let completed = complete_multipart_with_headers(
        &harness,
        "sse-c-multipart.bin",
        &upload_id,
        &[(1, part_etag)],
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(completed.status(), StatusCode::OK);
    assert!(
        completed
            .text()
            .await
            .expect("SSE-C complete body")
            .contains("<CompleteMultipartUploadResult>")
    );
    let root_ciphertext = harness
        .captured_add_file_bytes()
        .into_iter()
        .nth(1)
        .expect("captured encrypted multipart root");
    assert_ne!(
        root_ciphertext, plaintext,
        "SSE-C multipart root Kubo add must receive ciphertext"
    );
    harness.set_cat_body(root_cid, root_ciphertext);

    let latest = store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "sse-c-multipart.bin",
    )
    .await
    .expect("SSE-C completed multipart object");
    assert_eq!(latest.cid, root_cid);
    assert!(latest.encrypted);
    assert!(latest.key_wrap.is_none());

    let get =
        signed_get_with_headers(&harness, "sse-c-multipart.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(
        get.bytes()
            .await
            .expect("SSE-C multipart GET body")
            .as_ref(),
        plaintext
    );
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err(),
        "completed multipart upload row must be removed"
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("completed multipart parts")
            .is_empty(),
        "completed multipart part rows must be removed"
    );
    assert_eq!(
        kubo_call_counts(&harness).await,
        (2, 3, 2, 0),
        "SSE-C Complete pre-authenticates its part, cats it again to build the root, then GET cats the root"
    );
    assert_eq!(
        kubo_query_args(&harness, "/api/v0/cat").await,
        vec![
            part_cid.to_owned(),
            part_cid.to_owned(),
            root_cid.to_owned(),
        ],
        "SSE-C multipart must cat its part twice and its root once"
    );
    assert_pin_calls(&harness, "/api/v0/pin/add", &[part_cid, root_cid], &[]).await;
    assert!(
        kubo_query_args(&harness, "/api/v0/pin/rm").await.is_empty(),
        "SSE-C multipart success must not unpin"
    );
}

#[tokio::test]
async fn v03_put_object_cid_header_matrix() {
    let plaintext_harness = start_harness(scripted(&["QmV03PlainPut"], vec![])).await;
    let plaintext_put = signed_put(
        &plaintext_harness,
        "plain-cid.bin",
        &[],
        b"plain CID header payload".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(plaintext_put.status(), StatusCode::OK);
    assert_put_cid_headers(&plaintext_put, "QmV03PlainPut");

    let sse_s3_harness = start_harness(scripted(&["QmV03SseS3Put"], vec![])).await;
    let mut sse_s3_headers = HeaderMap::new();
    sse_s3_headers.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );
    let sse_s3_put = signed_put(
        &sse_s3_harness,
        "sse-s3-cid.bin",
        &[],
        b"SSE-S3 CID header payload".to_vec(),
        sse_s3_headers,
    )
    .await;
    assert_eq!(sse_s3_put.status(), StatusCode::OK);
    assert_put_cid_headers(&sse_s3_put, "QmV03SseS3Put");

    let sse_c_harness = start_harness(scripted(&["QmV03SseCPut"], vec![])).await;
    let sse_c_put = signed_put(
        &sse_c_harness,
        "sse-c-cid.bin",
        &[],
        b"SSE-C CID header payload".to_vec(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(sse_c_put.status(), StatusCode::OK);
    assert_put_cid_headers(&sse_c_put, "QmV03SseCPut");
}

#[tokio::test]
async fn v03_random_nonce_retries_and_part_replacement_never_reuse_nonce() {
    let plaintext = b"identical encrypted multipart payload".to_vec();
    let harness = start_harness(scripted(
        &[
            "QmNoncePart1",
            "QmNoncePart2",
            "QmNonceRoot1",
            "QmNonceRoot2",
        ],
        vec![],
    ))
    .await;
    let upload_id =
        create_multipart_with_headers(&harness, "nonce.bin", &[], sse_c_headers_for([7; 32])).await;

    upload_part_with_headers(
        &harness,
        "nonce.bin",
        &upload_id,
        1,
        plaintext.clone(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    let replacement_etag = upload_part_with_headers(
        &harness,
        "nonce.bin",
        &upload_id,
        1,
        plaintext.clone(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    let part_ciphertext = harness
        .captured_add_file_bytes()
        .get(1)
        .cloned()
        .expect("replacement ciphertext");
    harness.set_cat_body("QmNoncePart2", part_ciphertext);

    for _ in 0..2 {
        ipfs_s3_gateway::s3::ops::multipart::complete_multipart_upload_inner(
            &harness.state,
            inner_complete_request(
                &harness,
                "nonce.bin",
                &upload_id,
                &replacement_etag,
                sse_c_headers_for([7; 32]),
            ),
        )
        .await
        .expect("retryable CompleteMultipartUpload inner result");
    }

    let captured = harness.captured_add_file_bytes();
    assert_eq!(captured.len(), 4);
    let key = ipfs_s3_gateway::crypto::ObjectKey { bytes: [7; 32] };
    let mut nonces = std::collections::HashSet::new();
    for ciphertext in &captured {
        assert_eq!(
            ipfs_s3_gateway::crypto::aes_gcm::decrypt_chunk(&key, ciphertext)
                .expect("captured frame decrypts")
                .as_ref(),
            plaintext
        );
        nonces.insert(<[u8; 12]>::try_from(&ciphertext[..12]).unwrap());
    }
    assert_eq!(nonces.len(), captured.len());
}

#[tokio::test]
async fn fingerprinted_sse_c_get_head_wrong_key_is_zero_kubo_access_denied() {
    let harness = start_harness(scripted(&[], vec![])).await;
    seed_sse_c_object(
        &harness,
        "fingerprinted.bin",
        "QmFingerprinted",
        b"fingerprinted body",
        true,
        18,
    )
    .await;

    let get =
        signed_get_with_headers(&harness, "fingerprinted.bin", sse_c_headers_for([8; 32])).await;
    assert_s3_error(get, StatusCode::FORBIDDEN, "AccessDenied", "").await;
    let head =
        signed_head_with_headers(&harness, "fingerprinted.bin", sse_c_headers_for([8; 32])).await;
    assert_eq!(head.status(), StatusCode::FORBIDDEN);
    assert_eq!(kubo_call_counts(&harness).await, (0, 0, 0, 0));
}

#[tokio::test]
async fn legacy_sse_c_get_claims_after_exact_authentication_and_streams_second_cat() {
    let plaintext = b"legacy get body";
    let harness = start_harness(scripted(&[], vec![])).await;
    seed_sse_c_object(
        &harness,
        "legacy-get.bin",
        "QmLegacyGet",
        plaintext,
        false,
        i64::try_from(plaintext.len()).unwrap(),
    )
    .await;

    let response =
        signed_get_with_headers(&harness, "legacy-get.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap().as_ref(), plaintext);

    let object =
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "legacy-get.bin")
            .await
            .unwrap();
    assert!(object.sse_c_key_fingerprint.is_some());
    assert_eq!(kubo_call_counts(&harness).await, (0, 2, 0, 0));
}

#[tokio::test]
async fn legacy_sse_c_head_and_head_range_authenticate_once_then_zero_kubo() {
    let plaintext = b"legacy head body";
    let harness = start_harness(scripted(&[], vec![])).await;
    for (key, cid) in [
        ("legacy-head.bin", "QmLegacyHead"),
        ("legacy-head-range.bin", "QmLegacyHeadRange"),
    ] {
        seed_sse_c_object(
            &harness,
            key,
            cid,
            plaintext,
            false,
            i64::try_from(plaintext.len()).unwrap(),
        )
        .await;
    }

    let first =
        signed_head_with_headers(&harness, "legacy-head.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(first.status(), StatusCode::OK);
    let mut range_headers = sse_c_headers_for([7; 32]);
    range_headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=1-4"));
    let first_range =
        signed_head_with_headers(&harness, "legacy-head-range.bin", range_headers.clone()).await;
    assert_eq!(first_range.status(), StatusCode::OK);
    assert_eq!(first_range.headers()[http::header::CONTENT_LENGTH], "4");
    assert!(first_range.bytes().await.unwrap().is_empty());
    assert_eq!(kubo_call_counts(&harness).await, (0, 2, 0, 0));

    let repeated =
        signed_head_with_headers(&harness, "legacy-head.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(repeated.status(), StatusCode::OK);
    let repeated_range =
        signed_head_with_headers(&harness, "legacy-head-range.bin", range_headers).await;
    assert_eq!(repeated_range.status(), StatusCode::OK);
    assert_eq!(kubo_call_counts(&harness).await, (0, 2, 0, 0));
}

#[tokio::test]
async fn legacy_sse_c_wrong_key_or_size_mismatch_never_claims() {
    let plaintext = b"legacy authentication";
    let harness = start_harness(scripted(&[], vec![])).await;
    seed_sse_c_object(
        &harness,
        "legacy-wrong-key.bin",
        "QmLegacyWrongKey",
        plaintext,
        false,
        i64::try_from(plaintext.len()).unwrap(),
    )
    .await;
    seed_sse_c_object(
        &harness,
        "legacy-wrong-size.bin",
        "QmLegacyWrongSize",
        plaintext,
        false,
        i64::try_from(plaintext.len() + 1).unwrap(),
    )
    .await;
    seed_sse_c_object(&harness, "legacy-empty.bin", "QmLegacyEmpty", b"", false, 0).await;

    let wrong_key =
        signed_get_with_headers(&harness, "legacy-wrong-key.bin", sse_c_headers_for([8; 32])).await;
    assert_s3_error(wrong_key, StatusCode::FORBIDDEN, "AccessDenied", "").await;
    let wrong_size = signed_head_with_headers(
        &harness,
        "legacy-wrong-size.bin",
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(wrong_size.status(), StatusCode::FORBIDDEN);
    let empty =
        signed_head_with_headers(&harness, "legacy-empty.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(empty.status(), StatusCode::FORBIDDEN);

    for key in [
        "legacy-wrong-key.bin",
        "legacy-wrong-size.bin",
        "legacy-empty.bin",
    ] {
        assert!(
            store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
                .await
                .unwrap()
                .sse_c_key_fingerprint
                .is_none(),
            "{key} must remain unclaimed"
        );
    }
}

#[tokio::test]
async fn sse_c_get_and_head_return_customer_response_fields() {
    let plaintext = b"response fields";
    let harness = start_harness(scripted(&[], vec![])).await;
    seed_sse_c_object(
        &harness,
        "response-fields.bin",
        "QmResponseFields",
        plaintext,
        true,
        i64::try_from(plaintext.len()).unwrap(),
    )
    .await;
    let expected_md5 = base64::engine::general_purpose::STANDARD.encode(md5::compute([7; 32]).0);

    let get =
        signed_get_with_headers(&harness, "response-fields.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(
        get.headers()["x-amz-server-side-encryption-customer-algorithm"],
        "AES256"
    );
    assert_eq!(
        get.headers()["x-amz-server-side-encryption-customer-key-md5"],
        expected_md5
    );
    assert_eq!(get.bytes().await.unwrap().as_ref(), plaintext);

    let head =
        signed_head_with_headers(&harness, "response-fields.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(
        head.headers()["x-amz-server-side-encryption-customer-algorithm"],
        "AES256"
    );
    assert_eq!(
        head.headers()["x-amz-server-side-encryption-customer-key-md5"],
        expected_md5
    );
}

#[tokio::test]
async fn copy_sse_c_source_headers_are_required_and_wrong_key_never_publishes() {
    let plaintext = b"copy source";
    let harness = start_harness(scripted(&[], vec![])).await;
    seed_sse_c_object(
        &harness,
        "copy-source.bin",
        "QmCopySource",
        plaintext,
        true,
        i64::try_from(plaintext.len()).unwrap(),
    )
    .await;

    let valid = copy_source_sse_c_headers_for([7; 32]);
    let mut malformed = Vec::new();
    for missing in [
        "x-amz-copy-source-server-side-encryption-customer-algorithm",
        "x-amz-copy-source-server-side-encryption-customer-key",
        "x-amz-copy-source-server-side-encryption-customer-key-md5",
    ] {
        let mut headers = valid.clone();
        headers.remove(missing);
        malformed.push(headers);
    }
    let mut wrong_algorithm = valid.clone();
    wrong_algorithm.insert(
        "x-amz-copy-source-server-side-encryption-customer-algorithm",
        HeaderValue::from_static("AES128"),
    );
    malformed.push(wrong_algorithm);
    let mut invalid_base64 = valid.clone();
    invalid_base64.insert(
        "x-amz-copy-source-server-side-encryption-customer-key",
        HeaderValue::from_static("not-base64"),
    );
    malformed.push(invalid_base64);
    let mut short_key = valid.clone();
    short_key.insert(
        "x-amz-copy-source-server-side-encryption-customer-key",
        HeaderValue::from_str(&base64::engine::general_purpose::STANDARD.encode([7; 31])).unwrap(),
    );
    malformed.push(short_key);
    let mut wrong_md5 = valid.clone();
    wrong_md5.insert(
        "x-amz-copy-source-server-side-encryption-customer-key-md5",
        HeaderValue::from_str(&base64::engine::general_purpose::STANDARD.encode([0; 16])).unwrap(),
    );
    malformed.push(wrong_md5);
    malformed.push(sse_c_headers_for([7; 32]));

    for (index, headers) in malformed.into_iter().enumerate() {
        let destination = format!("invalid-copy-{index}.bin");
        let import_id = format!("invalid-copy-import-{index}");
        seed_running_import(&harness, &import_id, &destination, None).await;
        let response = signed_copy(&harness, "copy-source.bin", &destination, headers).await;
        assert_s3_error(response, StatusCode::BAD_REQUEST, "InvalidArgument", "").await;
        assert_import_state(&harness, &import_id, "running").await;
        assert_latest_absent(&harness, &destination).await;
    }

    seed_running_import(
        &harness,
        "wrong-key-copy-import",
        "wrong-key-copy.bin",
        None,
    )
    .await;
    let response = signed_copy(
        &harness,
        "copy-source.bin",
        "wrong-key-copy.bin",
        copy_source_sse_c_headers_for([8; 32]),
    )
    .await;
    assert_s3_error(response, StatusCode::FORBIDDEN, "AccessDenied", "").await;
    assert_import_state(&harness, "wrong-key-copy-import", "running").await;
    assert_latest_absent(&harness, "wrong-key-copy.bin").await;
    assert_eq!(kubo_call_counts(&harness).await, (0, 0, 0, 0));
}

#[tokio::test]
async fn legacy_sse_c_copy_claims_then_copies_fingerprint() {
    let plaintext = b"legacy copy source";
    let harness = start_harness(scripted(&[], vec![])).await;
    seed_sse_c_object(
        &harness,
        "legacy-copy-source.bin",
        "QmLegacyCopySource",
        plaintext,
        false,
        i64::try_from(plaintext.len()).unwrap(),
    )
    .await;

    let response = signed_copy(
        &harness,
        "legacy-copy-source.bin",
        "legacy-copy-destination.bin",
        copy_source_sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let source = store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "legacy-copy-source.bin",
    )
    .await
    .unwrap();
    let destination = store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "legacy-copy-destination.bin",
    )
    .await
    .unwrap();
    assert!(source.sse_c_key_fingerprint.is_some());
    assert_eq!(
        destination.sse_c_key_fingerprint,
        source.sse_c_key_fingerprint
    );
    assert_eq!(kubo_call_counts(&harness).await, (0, 1, 1, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_sse_c_copy_admits_destination_before_cat() {
    let plaintext = b"blocked legacy copy source";
    let (harness, mut kubo_block) =
        start_blocking_harness(scripted(&[], vec![]), KuboBlockTarget::Cat).await;
    seed_sse_c_object(
        &harness,
        "blocked-legacy-copy-source.bin",
        "QmBlockedLegacyCopySource",
        plaintext,
        false,
        i64::try_from(plaintext.len()).unwrap(),
    )
    .await;
    seed_running_import(
        &harness,
        "blocked-legacy-copy-import",
        "blocked-legacy-copy-destination.bin",
        None,
    )
    .await;

    let endpoint = OwnedTestEndpoint::from(&harness);
    let copy_task = tokio::spawn(async move {
        signed_copy(
            &endpoint,
            "blocked-legacy-copy-source.bin",
            "blocked-legacy-copy-destination.bin",
            copy_source_sse_c_headers_for([7; 32]),
        )
        .await
    });
    kubo_block.wait_until_blocked().await;
    assert_import_state(&harness, "blocked-legacy-copy-import", "superseded").await;

    kubo_block.release();
    let response = copy_task.await.expect("join blocked legacy CopyObject");
    assert_eq!(response.status(), StatusCode::OK);
    let source = store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "blocked-legacy-copy-source.bin",
    )
    .await
    .expect("load claimed legacy source");
    let destination = store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "blocked-legacy-copy-destination.bin",
    )
    .await
    .expect("load copied destination");
    assert!(source.sse_c_key_fingerprint.is_some());
    assert_eq!(
        destination.sse_c_key_fingerprint,
        source.sse_c_key_fingerprint
    );
}

#[tokio::test]
async fn all_object_publication_paths_and_completion_reconciliation_keep_fingerprint() {
    let harness = start_harness(scripted(
        &[
            "QmPlainPublication",
            "QmSseS3Publication",
            "QmSseCPublication",
            "QmPartPublication",
            "QmRootPublication",
        ],
        vec![],
    ))
    .await;
    assert_eq!(
        signed_put(
            &harness,
            "plain-publication.bin",
            &[],
            b"plain".to_vec(),
            HeaderMap::new(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    let mut sse_s3 = HeaderMap::new();
    sse_s3.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );
    assert_eq!(
        signed_put(
            &harness,
            "sse-s3-publication.bin",
            &[],
            b"sse-s3".to_vec(),
            sse_s3,
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_put(
            &harness,
            "sse-c-publication.bin",
            &[],
            b"sse-c".to_vec(),
            sse_c_headers_for([7; 32]),
        )
        .await
        .status(),
        StatusCode::OK
    );
    for (key, expected) in [
        ("plain-publication.bin", false),
        ("sse-s3-publication.bin", false),
        ("sse-c-publication.bin", true),
    ] {
        assert_eq!(
            store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
                .await
                .unwrap()
                .sse_c_key_fingerprint
                .is_some(),
            expected,
            "publication path {key}"
        );
    }

    let upload_id = create_multipart_with_headers(
        &harness,
        "complete-publication.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;
    let upload_fingerprint = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .unwrap()
        .sse_c_key_fingerprint
        .unwrap();
    let part_etag = upload_part_with_headers(
        &harness,
        "complete-publication.bin",
        &upload_id,
        1,
        b"multipart publication".to_vec(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    let part_ciphertext = harness
        .captured_add_file_bytes()
        .get(3)
        .cloned()
        .expect("multipart publication part ciphertext");
    harness.set_cat_body("QmPartPublication", part_ciphertext);
    let complete = complete_multipart_with_headers(
        &harness,
        "complete-publication.bin",
        &upload_id,
        &[(1, part_etag)],
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(complete.status(), StatusCode::OK);
    assert_eq!(
        store::object::get_latest(
            harness.state.store.db(),
            &harness.bucket,
            "complete-publication.bin",
        )
        .await
        .unwrap()
        .sse_c_key_fingerprint
        .as_deref(),
        Some(upload_fingerprint.as_str())
    );
}
