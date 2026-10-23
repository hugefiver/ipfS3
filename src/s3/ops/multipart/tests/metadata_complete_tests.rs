use super::*;
use wiremock::matchers::query_param;

fn selected_request(parts: &[(i32, &str)]) -> S3Request<CompleteMultipartUploadInput> {
    let mut request = complete_request("upload-selected", "unused");
    request.input.multipart_upload.as_mut().unwrap().parts = Some(
        parts
            .iter()
            .map(|&(number, etag)| CompletedPart {
                part_number: Some(number),
                e_tag: Some(ETag::Strong(etag.to_owned())),
                ..Default::default()
            })
            .collect(),
    );
    request
}

async fn seed_selected_parts(state: &Arc<AppState>) {
    seed_plain_upload(state, "upload-selected").await;
    for (number, cid, size) in [
        (1, "QmFirst", 5 * 1024 * 1024),
        (2, "QmUnselected", 13),
        (3, "QmLast", 4),
    ] {
        crate::store::multipart::upsert_part(
            state.store.db(),
            "upload-selected",
            number,
            cid,
            size,
            cid,
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn complete_batches_only_selected_parts_and_concatenates_in_client_order() {
    let kubo = multipart_kubo(&["QmSelectedRoot"]).await;
    for (cid, body) in [
        ("QmFirst", vec![b'A'; 5 * 1024 * 1024]),
        ("QmLast", b"end!".to_vec()),
    ] {
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .and(query_param("arg", cid))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
            .expect(1)
            .mount(&kubo)
            .await;
    }
    let state = test_state_with_bucket_and_kubo("test-bucket", kubo.uri()).await;
    seed_selected_parts(&state).await;
    let response =
        complete_multipart_upload(&state, selected_request(&[(1, "QmFirst"), (3, "QmLast")]))
            .await
            .unwrap();
    assert_eq!(response.output.bucket.as_deref(), Some("test-bucket"));
    assert_eq!(response.output.key.as_deref(), Some("archive.zip"));
    assert_eq!(response.output.e_tag.unwrap().value(), "QmSelectedRoot");
    let object = crate::store::object::get_latest(state.store.db(), "test-bucket", "archive.zip")
        .await
        .unwrap();
    assert_eq!(object.size, 5 * 1024 * 1024 + 4);
    assert_eq!(object.etag, "QmSelectedRoot");
    let requests = kubo.received_requests().await.unwrap();
    let add = requests
        .iter()
        .find(|request| request.url.path() == "/api/v0/add")
        .unwrap();
    assert!(add.body.windows(8).any(|bytes| bytes == b"AAAAend!"));
    assert!(!requests.iter().any(|request| {
        request
            .url
            .query_pairs()
            .any(|(name, value)| name == "arg" && value == "QmUnselected")
    }));
    assert!(matches!(
        crate::store::multipart::get_upload(state.store.db(), "upload-selected").await,
        Err(crate::error::AppError::NoSuchUpload(_))
    ));
}

#[tokio::test]
async fn complete_batch_lookup_preserves_part_validation_before_any_kubo_io() {
    let kubo = MockServer::start().await;
    let state = test_state_with_bucket_and_kubo("test-bucket", kubo.uri()).await;
    seed_selected_parts(&state).await;
    for (parts, code) in [
        (vec![(1, "wrong"), (9, "missing")], "InvalidPart"),
        (vec![(1, "QmFirst"), (9, "missing")], "InvalidPart"),
        (vec![(3, "QmLast"), (1, "QmFirst")], "InvalidPartOrder"),
        (vec![(1, "QmFirst"), (1, "QmFirst")], "InvalidPartOrder"),
        (vec![(2, "QmUnselected"), (3, "QmLast")], "EntityTooSmall"),
    ] {
        let error = complete_multipart_upload_inner(&state, selected_request(&parts))
            .await
            .unwrap_err();
        assert_eq!(error.code().as_str(), code);
        assert!(
            crate::store::multipart::get_upload(state.store.db(), "upload-selected")
                .await
                .is_ok()
        );
        assert_eq!(
            crate::store::multipart::list_parts(state.store.db(), "upload-selected")
                .await
                .unwrap()
                .len(),
            3
        );
    }
    assert!(kubo.received_requests().await.unwrap().is_empty());
}
