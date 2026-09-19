use std::collections::HashMap;

use http::{HeaderMap, HeaderValue, StatusCode};
use sea_orm::{EntityTrait, PaginatorTrait};

use super::{
    decompress::{
        AddReply, KuboScript, complete_multipart_with_headers, create_multipart,
        legal_single_entry_zip, start_harness, upload_part,
    },
    lifecycle::start_lifecycle_harness,
    sigv4::send_sigv4,
};

#[tokio::test]
async fn signed_import_rejects_nonstandard_class_without_creating_jobs() {
    let harness = super::import::start_strict_import_harness().await;
    for class in ["STANDARD_IA", "GLACIER", "unknown"] {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-storage-class", HeaderValue::from_str(class).unwrap());
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/xml"),
        );
        let response = send_sigv4(
            reqwest::Method::POST, &harness.endpoint, &harness.bucket, "destination",
            &[("ipfs3-import", "")],
            b"<ImportObject><Source><CID>QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE</CID></Source></ImportObject>".to_vec(),
            headers, "test",
        ).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let xml = response.text().await.unwrap();
        assert!(xml.contains("<Code>InvalidRequest</Code>"), "{xml}");
        assert!(
            xml.contains("direct writes only support STANDARD storage class"),
            "{xml}"
        );
    }
    use ipfs_s3_gateway::store::entities::import_job;
    assert_eq!(
        import_job::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(harness.kubo_total_call_count(), 0);
    assert_eq!(harness.transport_calls(), 0);
    harness.shutdown().await;
}

#[tokio::test]
async fn signed_standard_put_copy_and_multipart_remain_hot() {
    const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    let harness = start_lifecycle_harness(KuboScript {
        add_replies: vec![super::decompress::AddReply::Ok(CID)],
        cat_bodies: HashMap::from([(CID.to_owned(), b"standard-content".to_vec())]),
    })
    .await;
    for operation in ["put", "copy", "multipart"] {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-storage-class", HeaderValue::from_static("STANDARD"));
        let (method, key, query) = match operation {
            "copy" => {
                headers.insert(
                    "x-amz-copy-source",
                    HeaderValue::from_str(&format!("/{}/source", harness.bucket)).unwrap(),
                );
                (reqwest::Method::PUT, "copied", vec![])
            }
            "multipart" => (reqwest::Method::POST, "multipart", vec![("uploads", "")]),
            _ => (reqwest::Method::PUT, "source", vec![]),
        };
        let response = send_sigv4(
            method,
            &harness.endpoint,
            &harness.bucket,
            key,
            &query,
            b"standard-content".to_vec(),
            headers,
            "test",
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "{operation}: {}",
            response.text().await.unwrap()
        );
        if operation != "multipart" {
            let response = send_sigv4(
                reqwest::Method::GET,
                &harness.endpoint,
                &harness.bucket,
                key,
                &[],
                Vec::new(),
                HeaderMap::new(),
                "test",
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()["x-amz-storage-class"], "STANDARD");
            assert_eq!(response.headers()[http::header::ETAG], format!("\"{CID}\""));
            assert_eq!(
                response.bytes().await.unwrap(),
                b"standard-content".as_slice()
            );
        }
    }
    super::residency::assert_hot_standard_residency_invariant(harness.state.store.db()).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn signed_direct_writes_reject_nonstandard_class_before_side_effects() {
    let harness = start_lifecycle_harness(KuboScript {
        add_replies: Vec::new(),
        cat_bodies: HashMap::new(),
    })
    .await;
    for class in ["STANDARD_IA", "GLACIER", "REDUCED_REDUNDANCY", "unknown"] {
        for operation in ["put", "copy", "multipart", "zip"] {
            let mut headers = HeaderMap::new();
            headers.insert("x-amz-storage-class", HeaderValue::from_str(class).unwrap());
            let (method, query) = match operation {
                "copy" => {
                    headers.insert(
                        "x-amz-copy-source",
                        HeaderValue::from_str(&format!("/{}/source", harness.bucket)).unwrap(),
                    );
                    (reqwest::Method::PUT, vec![])
                }
                "multipart" => (reqwest::Method::POST, vec![("uploads", "")]),
                "zip" => (reqwest::Method::PUT, vec![("decompress-zip", "")]),
                _ => (reqwest::Method::PUT, vec![]),
            };
            let response = send_sigv4(
                method,
                &harness.endpoint,
                &harness.bucket,
                "destination",
                &query,
                b"not consumed".to_vec(),
                headers,
                "test",
            )
            .await;
            let status = response.status();
            let xml = response.text().await.unwrap();
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "{operation}/{class}: {xml}"
            );
            assert!(
                xml.contains("<Code>InvalidRequest</Code>"),
                "{operation}/{class}: {xml}"
            );
            assert!(
                xml.contains("direct writes only support STANDARD storage class"),
                "{operation}/{class}: {xml}"
            );
        }
    }
    assert!(harness.kubo.received_requests().await.unwrap().is_empty());
    use ipfs_s3_gateway::store::entities::{import_job, multipart_upload, object};
    assert_eq!(
        object::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        multipart_upload::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        import_job::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn signed_decompress_multipart_complete_rejects_nonstandard_class_before_side_effects() {
    const PART_CID: &str = "QmStorageClassPart";
    const ROOT_CID: &str = "QmStorageClassRoot";
    const ENTRY_CID: &str = "QmStorageClassEntry";

    let archive = legal_single_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![
            AddReply::Ok(PART_CID),
            AddReply::Ok(ROOT_CID),
            AddReply::Ok(ENTRY_CID),
        ],
        cat_bodies: HashMap::from([
            (PART_CID.to_owned(), archive.clone()),
            (ROOT_CID.to_owned(), archive.clone()),
        ]),
    })
    .await;
    let key = "archive.zip";
    let upload_id = create_multipart(&harness, key, &[("decompress-zip", "expanded/")]).await;
    let etag = upload_part(&harness, key, &upload_id, 1, archive).await;

    let upload_before =
        ipfs_s3_gateway::store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .expect("multipart upload before rejected CompleteMultipartUpload");
    let parts_before =
        ipfs_s3_gateway::store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("multipart parts before rejected CompleteMultipartUpload");
    let kubo_requests_before = harness.kubo.received_requests().await.unwrap().len();
    use ipfs_s3_gateway::store::entities::object;
    let published_objects_before = object::Entity::find()
        .count(harness.state.store.db())
        .await
        .unwrap();

    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-storage-class",
        HeaderValue::from_static("STANDARD_IA"),
    );
    let response =
        complete_multipart_with_headers(&harness, key, &upload_id, &[(1, etag)], headers).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let xml = response.text().await.unwrap();
    assert!(xml.contains("<Code>InvalidRequest</Code>"), "{xml}");
    assert!(
        xml.contains("direct writes only support STANDARD storage class"),
        "{xml}"
    );

    assert_eq!(
        harness.kubo.received_requests().await.unwrap().len(),
        kubo_requests_before,
        "rejected CompleteMultipartUpload must not call Kubo"
    );
    assert_eq!(
        ipfs_s3_gateway::store::multipart::get_upload(harness.state.store.db(), &upload_id,)
            .await
            .expect("retained multipart upload"),
        upload_before,
        "rejected CompleteMultipartUpload must retain the upload unchanged"
    );
    assert_eq!(
        ipfs_s3_gateway::store::multipart::list_parts(harness.state.store.db(), &upload_id,)
            .await
            .expect("retained multipart parts"),
        parts_before,
        "rejected CompleteMultipartUpload must retain all parts unchanged"
    );
    assert_eq!(
        object::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        published_objects_before,
        "rejected CompleteMultipartUpload must not publish objects"
    );
}
