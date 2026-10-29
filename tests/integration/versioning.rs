use super::common::{
    IMPORT_CID, IMPORT_TEST_CID_V0, OwnedTestEndpoint, SINGLE_ENTRY_BYTES,
    assert_no_kubo_pin_removes, assert_s3_error, delete_headers, delete_identifiers_xml,
    import_result_count, import_url_xml, lease_targets, owner_leases, scripted,
    signed_complete_multipart, signed_copy, signed_copy_with_tagging,
    signed_create_multipart_upload_with_tagging, signed_create_multipart_zip_upload_with_tagging,
    signed_decompress_zip_put, signed_delete_bucket, signed_delete_object,
    signed_delete_object_tagging_version, signed_delete_object_version, signed_get,
    signed_get_object_tagging, signed_get_object_tagging_version, signed_get_version,
    signed_get_version_with_headers, signed_get_with_headers, signed_head,
    signed_head_with_headers, signed_list_object_versions, signed_list_objects, signed_put,
    signed_put_bucket_versioning, signed_put_bucket_versioning_xml,
    signed_put_object_tagging_version, signed_put_with_tagging, signed_upload_part,
    sse_c_headers_for, tagging_pairs, xml_element_text, xml_element_values,
};
use crate::support::decompress::{
    AddReply, KuboScript, S3TestEndpoint, legal_single_entry_zip, start_harness,
};
use crate::support::import::{
    ImportHarnessConfig, TestHttpsReply, start_import_harness, wait_for_import_state,
};
use crate::support::pinning::{PinningHarness, PinningHarnessConfig, start_pinning_harness};
use crate::support::sigv4::send_sigv4;
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::state::AppState;
use ipfs_s3_gateway::store;
use sea_orm::{ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

async fn signed_get_bucket_versioning(harness: &impl S3TestEndpoint) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::GET,
        harness.endpoint(),
        harness.bucket(),
        "",
        &[("versioning", "")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

async fn signed_head_version(
    harness: &impl S3TestEndpoint,
    key: &str,
    version_id: &str,
) -> reqwest::Response {
    signed_head_version_with_headers(harness, key, version_id, HeaderMap::new()).await
}

async fn signed_head_version_with_headers(
    harness: &impl S3TestEndpoint,
    key: &str,
    version_id: &str,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::HEAD,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[("versionId", version_id)],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

async fn signed_copy_version(
    harness: &impl S3TestEndpoint,
    source_key: &str,
    source_version_id: &str,
    destination_key: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-copy-source",
        HeaderValue::from_str(&format!(
            "/{}/{source_key}?versionId={source_version_id}",
            harness.bucket()
        ))
        .expect("copy source header"),
    );
    send_sigv4(
        reqwest::Method::PUT,
        harness.endpoint(),
        harness.bucket(),
        destination_key,
        &[],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

#[tokio::test]
async fn versioning_bucket_configuration_xml_mfa_and_missing_bucket_errors() {
    let harness = start_harness(scripted(&[], vec![])).await;

    let unversioned = signed_get_bucket_versioning(&harness).await;
    assert_eq!(unversioned.status(), StatusCode::OK);
    let unversioned_xml = unversioned.text().await.expect("unversioned XML");
    assert!(unversioned_xml.contains("VersioningConfiguration"));
    assert!(!unversioned_xml.contains("<Status>"));
    assert!(!unversioned_xml.contains("MfaDelete"));

    let enabled = signed_put_bucket_versioning(&harness, "Enabled").await;
    assert_eq!(enabled.status(), StatusCode::OK);
    let enabled_get = signed_get_bucket_versioning(&harness).await;
    assert_eq!(enabled_get.status(), StatusCode::OK);
    assert!(
        enabled_get
            .text()
            .await
            .expect("enabled XML")
            .contains("<Status>Enabled</Status>")
    );

    let suspended = signed_put_bucket_versioning(&harness, "Suspended").await;
    assert_eq!(suspended.status(), StatusCode::OK);
    let suspended_get = signed_get_bucket_versioning(&harness).await;
    assert_eq!(suspended_get.status(), StatusCode::OK);
    assert!(
        suspended_get
            .text()
            .await
            .expect("suspended XML")
            .contains("<Status>Suspended</Status>")
    );

    let mut mfa_headers = HeaderMap::new();
    mfa_headers.insert("x-amz-mfa", HeaderValue::from_static("serial 123456"));
    assert_s3_error(
        signed_put_bucket_versioning_xml(
            &harness,
            "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"
                .to_owned(),
            mfa_headers,
        )
        .await,
        StatusCode::BAD_REQUEST,
        "InvalidArgument",
        "MFA delete is not supported",
    )
    .await;
    assert_s3_error(
        signed_put_bucket_versioning_xml(
            &harness,
            "<VersioningConfiguration><Status>Enabled</Status><MfaDelete>Enabled</MfaDelete></VersioningConfiguration>"
                .to_owned(),
            HeaderMap::new(),
        )
        .await,
        StatusCode::BAD_REQUEST,
        "InvalidArgument",
        "MFA delete is not supported",
    )
    .await;
    let still_suspended = signed_get_bucket_versioning(&harness).await;
    assert_eq!(still_suspended.status(), StatusCode::OK);
    assert!(
        still_suspended
            .text()
            .await
            .expect("post-MFA versioning XML")
            .contains("<Status>Suspended</Status>")
    );

    let missing = OwnedTestEndpoint {
        endpoint: harness.endpoint.clone(),
        bucket: "missing-bucket".to_owned(),
    };
    assert_s3_error(
        signed_get_bucket_versioning(&missing).await,
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        "",
    )
    .await;
}

#[tokio::test]
async fn versioning_list_uses_first_unreturned_pair_and_hides_markers_from_ordinary_list() {
    let harness = start_harness(scripted(&["QmListOld", "QmListNew"], vec![])).await;
    assert_eq!(
        signed_put_bucket_versioning(&harness, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );
    let old = signed_put(
        &harness,
        "listed.txt",
        &[],
        b"old".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(old.status(), StatusCode::OK);
    let old_version = old.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let new = signed_put(
        &harness,
        "listed.txt",
        &[],
        b"new".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(new.status(), StatusCode::OK);
    let new_version = new.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let deleted = signed_delete_object(&harness, "listed.txt").await;
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    let marker_version = deleted.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();

    let first = signed_list_object_versions(&harness, &[("max-keys", "1")]).await;
    assert_eq!(first.status(), StatusCode::OK);
    let first_xml = first.text().await.expect("first version page XML");
    assert!(first_xml.contains("<IsTruncated>true</IsTruncated>"));
    assert!(first_xml.contains("<DeleteMarker>"));
    assert!(first_xml.contains(&format!("<VersionId>{marker_version}</VersionId>")));
    assert!(!first_xml.contains(&format!("<VersionId>{new_version}</VersionId>")));
    let next_key = xml_element_values(&first_xml, "NextKeyMarker")[0].to_owned();
    let next_version = xml_element_values(&first_xml, "NextVersionIdMarker")[0].to_owned();
    assert_eq!(next_key, "listed.txt");
    assert_eq!(next_version, new_version);

    let second = signed_list_object_versions(
        &harness,
        &[
            ("max-keys", "1"),
            ("key-marker", &next_key),
            ("version-id-marker", &next_version),
        ],
    )
    .await;
    assert_eq!(second.status(), StatusCode::OK);
    let second_xml = second.text().await.expect("second version page XML");
    assert!(second_xml.contains(&format!("<VersionId>{new_version}</VersionId>")));
    let third_version = xml_element_values(&second_xml, "NextVersionIdMarker")[0].to_owned();
    assert_eq!(third_version, old_version);

    let ordinary = signed_list_objects(&harness).await;
    assert_eq!(ordinary.status(), StatusCode::OK);
    let ordinary_xml = ordinary.text().await.expect("ordinary list XML");
    assert!(!ordinary_xml.contains("listed.txt"));
    assert!(!ordinary_xml.contains("DeleteMarker"));
}

#[tokio::test]
async fn versioning_read_and_copy_select_current_and_exact_versions() {
    let old_body = b"historical body".to_vec();
    let new_body = b"current body".to_vec();
    let harness = start_harness(scripted(
        &["QmVersionOld", "QmVersionNew"],
        vec![
            ("QmVersionOld", old_body.clone()),
            ("QmVersionNew", new_body.clone()),
        ],
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&harness, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );

    let old = signed_put(
        &harness,
        "versioned.txt",
        &[],
        old_body.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(old.status(), StatusCode::OK);
    let old_version = old
        .headers()
        .get("x-amz-version-id")
        .expect("old version header")
        .to_str()
        .expect("old version header text")
        .to_owned();

    let current = signed_put(
        &harness,
        "versioned.txt",
        &[],
        new_body.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(current.status(), StatusCode::OK);
    let current_version = current
        .headers()
        .get("x-amz-version-id")
        .expect("current version header")
        .to_str()
        .expect("current version header text")
        .to_owned();

    let current_get = signed_get(&harness, "versioned.txt").await;
    assert_eq!(current_get.status(), StatusCode::OK);
    assert_eq!(current_get.headers()["x-amz-version-id"], current_version);
    assert_eq!(
        current_get
            .bytes()
            .await
            .expect("current GET body")
            .as_ref(),
        new_body
    );
    let current_head = signed_head(&harness, "versioned.txt", None).await;
    assert_eq!(current_head.status(), StatusCode::OK);
    assert_eq!(current_head.headers()["x-amz-version-id"], current_version);

    let historical_get = signed_get_version(&harness, "versioned.txt", &old_version).await;
    assert_eq!(historical_get.status(), StatusCode::OK);
    assert_eq!(historical_get.headers()["x-amz-version-id"], old_version);
    assert_eq!(
        historical_get
            .bytes()
            .await
            .expect("historical GET body")
            .as_ref(),
        old_body
    );
    let historical_head = signed_head_version(&harness, "versioned.txt", &old_version).await;
    assert_eq!(historical_head.status(), StatusCode::OK);
    assert_eq!(historical_head.headers()["x-amz-version-id"], old_version);

    let copy = signed_copy_version(
        &harness,
        "versioned.txt",
        &old_version,
        "copied-historical.txt",
    )
    .await;
    assert_eq!(copy.status(), StatusCode::OK);
    assert_eq!(copy.headers()["x-amz-copy-source-version-id"], old_version);
    let copied_version = copy
        .headers()
        .get("x-amz-version-id")
        .expect("copied object version ID")
        .to_str()
        .expect("copied object version header text")
        .to_owned();
    assert_ne!(copied_version, old_version);
    let copied = signed_get(&harness, "copied-historical.txt").await;
    assert_eq!(copied.status(), StatusCode::OK);
    assert_eq!(copied.headers()["x-amz-version-id"], copied_version);
    assert_eq!(
        copied.bytes().await.expect("copied GET body").as_ref(),
        old_body
    );
}

#[tokio::test]
async fn versioning_tagging_selects_exact_content_and_reports_marker_errors() {
    let harness = start_harness(scripted(
        &["QmTaggingHistorical", "QmTaggingCurrent"],
        vec![
            ("QmTaggingHistorical", b"historical tags".to_vec()),
            ("QmTaggingCurrent", b"current tags".to_vec()),
        ],
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&harness, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );

    let historical = signed_put_with_tagging(
        &harness,
        "versioned-tagging.txt",
        b"historical tags".to_vec(),
        "owner=historical",
    )
    .await;
    assert_eq!(historical.status(), StatusCode::OK);
    let historical_version = historical.headers()["x-amz-version-id"]
        .to_str()
        .expect("historical version ID")
        .to_owned();
    let current = signed_put_with_tagging(
        &harness,
        "versioned-tagging.txt",
        b"current tags".to_vec(),
        "owner=current",
    )
    .await;
    assert_eq!(current.status(), StatusCode::OK);
    let current_version = current.headers()["x-amz-version-id"]
        .to_str()
        .expect("current version ID")
        .to_owned();

    let historical_tags = signed_get_object_tagging_version(
        &harness,
        "versioned-tagging.txt",
        Some(&historical_version),
    )
    .await;
    assert_eq!(historical_tags.status(), StatusCode::OK);
    assert_eq!(
        historical_tags.headers()["x-amz-version-id"],
        historical_version
    );
    assert_eq!(
        tagging_pairs(
            &historical_tags
                .text()
                .await
                .expect("historical tagging XML")
        ),
        vec![("owner".to_owned(), "historical".to_owned())]
    );

    let put_exact = signed_put_object_tagging_version(
        &harness,
        "versioned-tagging.txt",
        &[("owner", "historical-replaced")],
        Some(&historical_version),
    )
    .await;
    assert_eq!(put_exact.status(), StatusCode::OK);
    assert_eq!(put_exact.headers()["x-amz-version-id"], historical_version);
    let historical_tags = signed_get_object_tagging_version(
        &harness,
        "versioned-tagging.txt",
        Some(&historical_version),
    )
    .await;
    assert_eq!(historical_tags.status(), StatusCode::OK);
    assert_eq!(
        tagging_pairs(&historical_tags.text().await.expect("replaced tagging XML")),
        vec![("owner".to_owned(), "historical-replaced".to_owned())]
    );
    let current_tags = signed_get_object_tagging(&harness, "versioned-tagging.txt").await;
    assert_eq!(current_tags.status(), StatusCode::OK);
    assert_eq!(current_tags.headers()["x-amz-version-id"], current_version);
    assert_eq!(
        tagging_pairs(&current_tags.text().await.expect("current tagging XML")),
        vec![("owner".to_owned(), "current".to_owned())]
    );

    let delete_exact = signed_delete_object_tagging_version(
        &harness,
        "versioned-tagging.txt",
        Some(&historical_version),
    )
    .await;
    assert_eq!(delete_exact.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        delete_exact.headers()["x-amz-version-id"],
        historical_version
    );
    let historical_tags = signed_get_object_tagging_version(
        &harness,
        "versioned-tagging.txt",
        Some(&historical_version),
    )
    .await;
    assert_eq!(historical_tags.status(), StatusCode::OK);
    assert!(tagging_pairs(&historical_tags.text().await.expect("empty tagging XML")).is_empty());

    let marker = signed_delete_object(&harness, "versioned-tagging.txt").await;
    assert_eq!(marker.status(), StatusCode::NO_CONTENT);
    let marker_version = marker.headers()["x-amz-version-id"]
        .to_str()
        .expect("marker version ID")
        .to_owned();
    let current_marker = signed_get_object_tagging(&harness, "versioned-tagging.txt").await;
    assert_eq!(current_marker.status(), StatusCode::NOT_FOUND);
    assert_eq!(current_marker.headers()["x-amz-delete-marker"], "true");
    assert_eq!(current_marker.headers()["x-amz-version-id"], marker_version);
    assert!(
        current_marker
            .text()
            .await
            .expect("current marker error XML")
            .contains("NoSuchKey")
    );
    let exact_marker =
        signed_get_object_tagging_version(&harness, "versioned-tagging.txt", Some(&marker_version))
            .await;
    assert_eq!(exact_marker.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(exact_marker.headers()["x-amz-delete-marker"], "true");
    assert_eq!(exact_marker.headers()["x-amz-version-id"], marker_version);
    assert!(
        exact_marker
            .headers()
            .get(http::header::LAST_MODIFIED)
            .is_some()
    );
    assert!(
        exact_marker
            .text()
            .await
            .expect("explicit marker error XML")
            .contains("MethodNotAllowed")
    );
    assert_s3_error(
        signed_get_object_tagging_version(
            &harness,
            "versioned-tagging.txt",
            Some("00000000-0000-0000-0000-000000000001"),
        )
        .await,
        StatusCode::NOT_FOUND,
        "NoSuchVersion",
        "version not found",
    )
    .await;
}

#[tokio::test]
async fn versioning_delete_promotes_content_and_orders_multi_delete_effects() {
    let old_body = b"delete old body".to_vec();
    let current_body = b"delete current body".to_vec();
    let batch_body = b"delete batch body".to_vec();
    let harness = start_harness(scripted(
        &["QmDeleteOld", "QmDeleteCurrent", "QmDeleteBatch"],
        vec![
            ("QmDeleteOld", old_body.clone()),
            ("QmDeleteCurrent", current_body.clone()),
            ("QmDeleteBatch", batch_body),
        ],
    ))
    .await;

    let missing_unversioned = signed_delete_object(&harness, "missing.txt").await;
    assert_eq!(missing_unversioned.status(), StatusCode::NO_CONTENT);
    assert!(
        missing_unversioned
            .headers()
            .get("x-amz-version-id")
            .is_none()
    );
    assert_eq!(
        signed_put_bucket_versioning(&harness, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );

    let old = signed_put(
        &harness,
        "version-delete.txt",
        &[],
        old_body.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(old.status(), StatusCode::OK);
    let old_version = old.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let current = signed_put(
        &harness,
        "version-delete.txt",
        &[],
        current_body.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(current.status(), StatusCode::OK);
    let current_version = current.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();

    let simple = signed_delete_object(&harness, "version-delete.txt").await;
    assert_eq!(simple.status(), StatusCode::NO_CONTENT);
    assert_eq!(simple.headers()["x-amz-delete-marker"], "true");
    let marker_version = simple.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    uuid::Uuid::parse_str(&marker_version).unwrap();

    let delete_marker =
        signed_delete_object_version(&harness, "version-delete.txt", Some(&marker_version)).await;
    assert_eq!(delete_marker.status(), StatusCode::NO_CONTENT);
    assert_eq!(delete_marker.headers()["x-amz-delete-marker"], "true");
    assert_eq!(delete_marker.headers()["x-amz-version-id"], marker_version);
    let promoted_current = signed_get(&harness, "version-delete.txt").await;
    assert_eq!(promoted_current.status(), StatusCode::OK);
    assert_eq!(
        promoted_current.bytes().await.unwrap().as_ref(),
        current_body
    );

    let delete_current =
        signed_delete_object_version(&harness, "version-delete.txt", Some(&current_version)).await;
    assert_eq!(delete_current.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        delete_current.headers()["x-amz-version-id"],
        current_version
    );
    assert!(
        delete_current
            .headers()
            .get("x-amz-delete-marker")
            .is_none()
    );
    let promoted_old = signed_get(&harness, "version-delete.txt").await;
    assert_eq!(promoted_old.status(), StatusCode::OK);
    assert_eq!(promoted_old.bytes().await.unwrap().as_ref(), old_body);

    assert_s3_error(
        signed_delete_object_version(
            &harness,
            "version-delete.txt",
            Some("00000000-0000-0000-0000-000000000001"),
        )
        .await,
        StatusCode::NOT_FOUND,
        "NoSuchVersion",
        "version not found",
    )
    .await;

    let batch = signed_put(
        &harness,
        "batch-version-delete.txt",
        &[],
        b"delete batch body".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(batch.status(), StatusCode::OK);
    let batch_version = batch.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let unknown = "00000000-0000-0000-0000-000000000002";
    let multi = signed_delete_object_versions(
        &harness,
        &[
            ("batch-version-delete.txt", None),
            ("batch-version-delete.txt", None),
            ("batch-version-delete.txt", Some(&batch_version)),
            ("invalid-version.txt", Some("not-a-version")),
            ("unknown-version.txt", Some(unknown)),
        ],
        false,
    )
    .await;
    assert_eq!(multi.status(), StatusCode::OK);
    let multi_xml = multi.text().await.unwrap();
    assert_eq!(
        xml_element_values(&multi_xml, "Key"),
        vec![
            "batch-version-delete.txt",
            "batch-version-delete.txt",
            "batch-version-delete.txt",
            "invalid-version.txt",
            "unknown-version.txt",
        ]
    );
    let marker_versions = xml_element_values(&multi_xml, "DeleteMarkerVersionId");
    assert_eq!(marker_versions.len(), 2);
    assert_ne!(marker_versions[0], marker_versions[1]);
    assert!(multi_xml.contains(&format!("<VersionId>{batch_version}</VersionId>")));
    assert!(multi_xml.contains("<Code>InvalidArgument</Code>"));
    assert!(multi_xml.contains("<Code>NoSuchVersion</Code>"));

    let before_quiet = store::entities::object_version::Entity::find()
        .filter(store::entities::object_version::Column::Bucket.eq(&harness.bucket))
        .filter(store::entities::object_version::Column::Key.eq("batch-version-delete.txt"))
        .count(harness.state.store.db())
        .await
        .unwrap();
    let quiet = signed_delete_object_versions(
        &harness,
        &[
            ("batch-version-delete.txt", None),
            ("batch-version-delete.txt", None),
            ("invalid-version.txt", Some("still-not-a-version")),
        ],
        true,
    )
    .await;
    assert_eq!(quiet.status(), StatusCode::OK);
    let quiet_xml = quiet.text().await.unwrap();
    assert!(!quiet_xml.contains("<Deleted>"));
    assert!(quiet_xml.contains("<Code>InvalidArgument</Code>"));
    assert_eq!(
        store::entities::object_version::Entity::find()
            .filter(store::entities::object_version::Column::Bucket.eq(&harness.bucket))
            .filter(store::entities::object_version::Column::Key.eq("batch-version-delete.txt"))
            .count(harness.state.store.db())
            .await
            .unwrap(),
        before_quiet + 2
    );
    assert!(
        harness
            .kubo
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.url.path() != "/api/v0/pin/rm")
    );

    let delete_old =
        signed_delete_object_version(&harness, "version-delete.txt", Some(&old_version)).await;
    assert_eq!(delete_old.status(), StatusCode::NO_CONTENT);
    assert_eq!(delete_old.headers()["x-amz-version-id"], old_version);
}

#[tokio::test]
async fn versioning_current_and_exact_plain_sse_s3_sse_c_read_matrix() {
    let plain_body = b"versioned plain body".to_vec();
    let sse_s3_body = b"versioned SSE-S3 body".to_vec();
    let sse_c_body = b"versioned SSE-C body".to_vec();
    let harness = start_harness(scripted(
        &["QmVersionPlain", "QmVersionSseS3", "QmVersionSseC"],
        vec![("QmVersionPlain", plain_body.clone())],
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&harness, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );

    let plain = signed_put(
        &harness,
        "encrypted-versions.bin",
        &[],
        plain_body.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(plain.status(), StatusCode::OK);
    let plain_version = plain.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();

    let mut sse_s3_headers = HeaderMap::new();
    sse_s3_headers.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );
    let sse_s3 = signed_put(
        &harness,
        "encrypted-versions.bin",
        &[],
        sse_s3_body.clone(),
        sse_s3_headers,
    )
    .await;
    assert_eq!(sse_s3.status(), StatusCode::OK);
    let sse_s3_version = sse_s3.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    harness.set_cat_body(
        "QmVersionSseS3",
        harness.captured_add_file_bytes()[1].clone(),
    );

    let sse_c = signed_put(
        &harness,
        "encrypted-versions.bin",
        &[],
        sse_c_body.clone(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(sse_c.status(), StatusCode::OK);
    let sse_c_version = sse_c.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    harness.set_cat_body(
        "QmVersionSseC",
        harness.captured_add_file_bytes()[2].clone(),
    );

    let cases = [
        (
            plain_version.as_str(),
            plain_body.as_slice(),
            HeaderMap::new(),
            None,
        ),
        (
            sse_s3_version.as_str(),
            sse_s3_body.as_slice(),
            HeaderMap::new(),
            Some("AES256"),
        ),
        (
            sse_c_version.as_str(),
            sse_c_body.as_slice(),
            sse_c_headers_for([7; 32]),
            None,
        ),
    ];
    for (version_id, expected_body, headers, expected_sse_s3) in cases {
        let get = signed_get_version_with_headers(
            &harness,
            "encrypted-versions.bin",
            version_id,
            headers.clone(),
        )
        .await;
        assert_eq!(get.status(), StatusCode::OK, "exact GET {version_id}");
        assert_eq!(get.headers()["x-amz-version-id"], version_id);
        assert_eq!(
            get.headers()
                .get("x-amz-server-side-encryption")
                .map(|value| value.to_str().unwrap()),
            expected_sse_s3
        );
        if !headers.is_empty() {
            assert_eq!(
                get.headers()["x-amz-server-side-encryption-customer-algorithm"],
                "AES256"
            );
        }
        assert_eq!(get.bytes().await.unwrap().as_ref(), expected_body);

        let head = signed_head_version_with_headers(
            &harness,
            "encrypted-versions.bin",
            version_id,
            headers,
        )
        .await;
        assert_eq!(head.status(), StatusCode::OK, "exact HEAD {version_id}");
        assert_eq!(head.headers()["x-amz-version-id"], version_id);
    }

    let current_get = signed_get_with_headers(
        &harness,
        "encrypted-versions.bin",
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(current_get.status(), StatusCode::OK);
    assert_eq!(current_get.headers()["x-amz-version-id"], sse_c_version);
    assert_eq!(current_get.bytes().await.unwrap().as_ref(), sse_c_body);
    let current_head = signed_head_with_headers(
        &harness,
        "encrypted-versions.bin",
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(current_head.status(), StatusCode::OK);
    assert_eq!(current_head.headers()["x-amz-version-id"], sse_c_version);
}

#[tokio::test]
async fn versioning_suspend_replaces_null_reenable_unique_and_bucket_cleanup() {
    let harness = start_harness(scripted(
        &[
            "QmSuspendOpaqueOne",
            "QmSuspendOpaqueTwo",
            "QmSuspendNullOne",
            "QmSuspendNullTwo",
            "QmSuspendReenabled",
        ],
        vec![],
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&harness, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );
    let mut opaque_ids = Vec::new();
    for body in [b"opaque one".as_slice(), b"opaque two".as_slice()] {
        let response = signed_put(
            &harness,
            "suspend.txt",
            &[],
            body.to_vec(),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        opaque_ids.push(
            response.headers()["x-amz-version-id"]
                .to_str()
                .unwrap()
                .to_owned(),
        );
    }
    assert_ne!(opaque_ids[0], opaque_ids[1]);

    assert_eq!(
        signed_put_bucket_versioning(&harness, "Suspended")
            .await
            .status(),
        StatusCode::OK
    );
    for body in [b"null one".as_slice(), b"null two".as_slice()] {
        let response = signed_put(
            &harness,
            "suspend.txt",
            &[],
            body.to_vec(),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-amz-version-id"], "null");
    }
    let suspended_rows =
        publication_matrix_rows(&harness.state, &harness.bucket, "suspend.txt").await;
    assert_eq!(suspended_rows.len(), 3);
    assert_eq!(
        suspended_rows
            .iter()
            .filter(|row| row.version_id.is_none())
            .count(),
        1
    );
    for opaque_id in &opaque_ids {
        assert!(
            suspended_rows
                .iter()
                .any(|row| row.version_id.as_deref() == Some(opaque_id.as_str()))
        );
    }

    let list = signed_list_object_versions(&harness, &[]).await;
    assert_eq!(
        list.status(),
        StatusCode::OK,
        "suspended version list status"
    );
    let xml = list.text().await.expect("suspended version list body");
    let version_count = xml.matches("<Version>").count();
    let delete_marker_count = xml.matches("<DeleteMarker>").count();
    let null_version_count = xml.matches("<VersionId>null</VersionId>").count();
    let latest_count = xml.matches("<IsLatest>true</IsLatest>").count();
    assert_eq!(
        version_count, 3,
        "unexpected Version entry count: {version_count}"
    );
    assert_eq!(
        delete_marker_count, 0,
        "unexpected DeleteMarker entry count: {delete_marker_count}"
    );
    assert_eq!(
        null_version_count, 1,
        "unexpected literal null VersionId count: {null_version_count}"
    );
    for opaque_id in &opaque_ids {
        let opaque_version_count = xml
            .matches(&format!("<VersionId>{opaque_id}</VersionId>"))
            .count();
        assert_eq!(
            opaque_version_count, 1,
            "unexpected opaque VersionId entry count: {opaque_version_count}"
        );
    }
    assert_eq!(
        latest_count, 1,
        "unexpected latest Version entry count: {latest_count}"
    );
    let latest_entry = xml
        .split("<Version>")
        .skip(1)
        .filter_map(|entry| entry.split_once("</Version>").map(|(entry, _)| entry))
        .find(|entry| entry.contains("<IsLatest>true</IsLatest>"))
        .expect("latest Version entry");
    assert!(
        latest_entry.contains("<VersionId>null</VersionId>"),
        "latest Version entry must use literal null VersionId"
    );

    let marker = signed_delete_object(&harness, "suspend.txt").await;
    assert_eq!(marker.status(), StatusCode::NO_CONTENT);
    assert_eq!(marker.headers()["x-amz-delete-marker"], "true");
    assert_eq!(marker.headers()["x-amz-version-id"], "null");
    let delete_marker = signed_delete_object_version(&harness, "suspend.txt", Some("null")).await;
    assert_eq!(delete_marker.status(), StatusCode::NO_CONTENT);
    assert_eq!(delete_marker.headers()["x-amz-delete-marker"], "true");
    let restored = signed_head(&harness, "suspend.txt", None).await;
    assert_eq!(restored.status(), StatusCode::OK);
    assert_eq!(restored.headers()["x-amz-version-id"], opaque_ids[1]);

    assert_eq!(
        signed_put_bucket_versioning(&harness, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );
    let reenabled = signed_put(
        &harness,
        "suspend.txt",
        &[],
        b"re-enabled".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(reenabled.status(), StatusCode::OK);
    let reenabled_id = reenabled.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    uuid::Uuid::parse_str(&reenabled_id).unwrap();
    assert!(!opaque_ids.contains(&reenabled_id));

    assert_s3_error(
        signed_delete_bucket(&harness).await,
        StatusCode::CONFLICT,
        "BucketNotEmpty",
        "",
    )
    .await;
    for version_id in [
        reenabled_id.as_str(),
        opaque_ids[1].as_str(),
        opaque_ids[0].as_str(),
    ] {
        let response =
            signed_delete_object_version(&harness, "suspend.txt", Some(version_id)).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
    assert!(
        publication_matrix_rows(&harness.state, &harness.bucket, "suspend.txt")
            .await
            .is_empty()
    );
    assert_eq!(
        signed_delete_bucket(&harness).await.status(),
        StatusCode::NO_CONTENT
    );
}

async fn marker_nonversion_side_effect_counts(state: &Arc<AppState>) -> [u64; 7] {
    [
        store::entities::object::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        store::entities::object_tag::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        store::entities::pin_lease::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        store::entities::pin_lease_target::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        store::entities::pin_job::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        store::entities::pin_provider_usage::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        store::entities::remote_pin::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
    ]
}

#[tokio::test]
async fn versioning_marker_errors_are_exact_zero_kubo_and_zero_content_side_effects() {
    let harness = start_harness(scripted(&["QmMarkerContent"], vec![])).await;
    assert_eq!(
        signed_put_bucket_versioning(&harness, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );
    let put = signed_put_with_tagging(
        &harness,
        "marker.txt",
        b"marker content".to_vec(),
        "fixture=marker",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let counts_before = marker_nonversion_side_effect_counts(&harness.state).await;
    let kubo_before = harness.kubo.received_requests().await.unwrap().len();

    let deleted = signed_delete_object(&harness, "marker.txt").await;
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    assert_eq!(deleted.headers()["x-amz-delete-marker"], "true");
    let marker_id = deleted.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    uuid::Uuid::parse_str(&marker_id).unwrap();
    assert_eq!(
        marker_nonversion_side_effect_counts(&harness.state).await,
        counts_before,
        "marker creation owns no content/tag/pin rows"
    );

    let current_get = signed_get(&harness, "marker.txt").await;
    assert_eq!(current_get.status(), StatusCode::NOT_FOUND);
    assert_eq!(current_get.headers()["x-amz-delete-marker"], "true");
    assert_eq!(current_get.headers()["x-amz-version-id"], marker_id);
    assert!(
        current_get
            .text()
            .await
            .unwrap()
            .contains("<Code>NoSuchKey</Code>")
    );
    let current_head = signed_head(&harness, "marker.txt", None).await;
    assert_eq!(current_head.status(), StatusCode::NOT_FOUND);
    assert_eq!(current_head.headers()["x-amz-delete-marker"], "true");
    assert_eq!(current_head.headers()["x-amz-version-id"], marker_id);

    let exact_get = signed_get_version(&harness, "marker.txt", &marker_id).await;
    assert_eq!(exact_get.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(exact_get.headers()["x-amz-delete-marker"], "true");
    assert_eq!(exact_get.headers()["x-amz-version-id"], marker_id);
    assert!(
        exact_get
            .headers()
            .get(http::header::LAST_MODIFIED)
            .is_some()
    );
    assert!(
        exact_get
            .text()
            .await
            .unwrap()
            .contains("<Code>MethodNotAllowed</Code>")
    );
    let exact_head = signed_head_version(&harness, "marker.txt", &marker_id).await;
    assert_eq!(exact_head.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(exact_head.headers()["x-amz-delete-marker"], "true");
    assert_eq!(exact_head.headers()["x-amz-version-id"], marker_id);
    assert!(
        exact_head
            .headers()
            .get(http::header::LAST_MODIFIED)
            .is_some()
    );

    let current_copy = signed_copy(
        &harness,
        "marker.txt",
        "marker-copy-current.txt",
        HeaderMap::new(),
    )
    .await;
    assert_eq!(current_copy.status(), StatusCode::NOT_FOUND);
    assert_eq!(current_copy.headers()["x-amz-delete-marker"], "true");
    assert!(current_copy.text().await.unwrap().contains("NoSuchKey"));
    let exact_copy =
        signed_copy_version(&harness, "marker.txt", &marker_id, "marker-copy-exact.txt").await;
    assert_eq!(exact_copy.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(exact_copy.headers()["x-amz-delete-marker"], "true");
    assert!(
        exact_copy
            .text()
            .await
            .unwrap()
            .contains("MethodNotAllowed")
    );

    let current_tags = signed_get_object_tagging(&harness, "marker.txt").await;
    assert_eq!(current_tags.status(), StatusCode::NOT_FOUND);
    let exact_tags =
        signed_get_object_tagging_version(&harness, "marker.txt", Some(&marker_id)).await;
    assert_eq!(exact_tags.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        marker_nonversion_side_effect_counts(&harness.state).await,
        counts_before
    );
    assert_eq!(
        harness.kubo.received_requests().await.unwrap().len(),
        kubo_before,
        "marker create/read/head/copy/tag paths never touch Kubo"
    );
}

#[tokio::test]
async fn versioning_signed_listing_url_delimiter_and_error_matrix() {
    let harness = start_harness(scripted(
        &[
            "QmListingSpaceOld",
            "QmListingSpaceNew",
            "QmListingNested",
            "QmListingZed",
        ],
        vec![],
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&harness, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );
    let mut spaced_versions = Vec::new();
    for body in [b"old".as_slice(), b"new".as_slice()] {
        let response = signed_put(
            &harness,
            "pre/a b.txt",
            &[],
            body.to_vec(),
            HeaderMap::new(),
        )
        .await;
        spaced_versions.push(
            response.headers()["x-amz-version-id"]
                .to_str()
                .unwrap()
                .to_owned(),
        );
    }
    for (key, body) in [
        ("pre/sub/c.txt", b"nested".as_slice()),
        ("z.txt", b"zed".as_slice()),
    ] {
        assert_eq!(
            signed_put(&harness, key, &[], body.to_vec(), HeaderMap::new())
                .await
                .status(),
            StatusCode::OK
        );
    }

    let list = signed_list_object_versions(
        &harness,
        &[
            ("prefix", "pre/"),
            ("delimiter", "/"),
            ("encoding-type", "url"),
            ("max-keys", "3"),
        ],
    )
    .await;
    assert_eq!(list.status(), StatusCode::OK);
    let xml = list.text().await.unwrap();
    assert!(xml.contains("<EncodingType>url</EncodingType>"));
    assert!(xml.contains("<Prefix>pre%2F</Prefix>"));
    assert!(xml.contains("<Delimiter>%2F</Delimiter>"));
    assert_eq!(xml.matches("<Key>pre%2Fa%20b.txt</Key>").count(), 2);
    assert!(xml.contains("<Prefix>pre%2Fsub%2F</Prefix>"));

    assert_s3_error(
        signed_list_object_versions(&harness, &[("version-id-marker", &spaced_versions[0])]).await,
        StatusCode::BAD_REQUEST,
        "InvalidArgument",
        "",
    )
    .await;
    assert_s3_error(
        signed_list_object_versions(
            &harness,
            &[
                ("key-marker", "pre/sub/c.txt"),
                ("version-id-marker", &spaced_versions[0]),
            ],
        )
        .await,
        StatusCode::BAD_REQUEST,
        "InvalidArgument",
        "",
    )
    .await;
    assert_s3_error(
        signed_get_version(&harness, "pre/a b.txt", "not-a-version").await,
        StatusCode::BAD_REQUEST,
        "InvalidArgument",
        "canonical UUID",
    )
    .await;
    assert_s3_error(
        signed_get_version(
            &harness,
            "pre/a b.txt",
            "00000000-0000-0000-0000-000000000001",
        )
        .await,
        StatusCode::NOT_FOUND,
        "NoSuchVersion",
        "",
    )
    .await;
    assert_s3_error(
        signed_get(&harness, "missing-key.txt").await,
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "",
    )
    .await;
    let missing_bucket = OwnedTestEndpoint {
        endpoint: harness.endpoint.clone(),
        bucket: "missing-version-bucket".to_owned(),
    };
    assert_s3_error(
        signed_list_object_versions(&missing_bucket, &[]).await,
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        "",
    )
    .await;

    let unversioned = start_harness(scripted(&[], vec![])).await;
    assert_s3_error(
        signed_get_version(&unversioned, "missing-key.txt", "null").await,
        StatusCode::BAD_REQUEST,
        "InvalidArgument",
        "unversioned bucket",
    )
    .await;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicationMatrixState {
    Unversioned,
    Enabled,
    Suspended,
}

impl PublicationMatrixState {
    fn label(self) -> &'static str {
        match self {
            Self::Unversioned => "unversioned",
            Self::Enabled => "enabled",
            Self::Suspended => "suspended",
        }
    }
}

#[derive(Debug)]
struct PublicationMatrixBefore {
    key: String,
    rows: Vec<store::entities::object_version::Model>,
}

#[derive(Debug)]
enum PublicationVersionSurface {
    Header(Option<String>),
    DiscoverOnly,
}

fn response_version_surface(response: &reqwest::Response) -> PublicationVersionSurface {
    PublicationVersionSurface::Header(
        response
            .headers()
            .get("x-amz-version-id")
            .map(|value| value.to_str().expect("version response header").to_owned()),
    )
}

async fn publication_matrix_rows(
    state: &Arc<AppState>,
    bucket: &str,
    key: &str,
) -> Vec<store::entities::object_version::Model> {
    store::entities::object_version::Entity::find()
        .filter(store::entities::object_version::Column::Bucket.eq(bucket))
        .filter(store::entities::object_version::Column::Key.eq(key))
        .order_by_asc(store::entities::object_version::Column::Sequence)
        .all(state.store.db())
        .await
        .expect("load publication matrix version rows")
}

async fn prepare_publication_matrix_state(
    harness: &impl S3TestEndpoint,
    state: &Arc<AppState>,
    matrix_state: PublicationMatrixState,
    keys_and_tagging: &[(&str, &str)],
) -> Vec<PublicationMatrixBefore> {
    if matrix_state == PublicationMatrixState::Enabled {
        let response = signed_put_bucket_versioning(harness, "Enabled").await;
        assert_eq!(response.status(), StatusCode::OK, "enable matrix bucket");
    }

    let publish_baselines = async {
        for (key, tagging) in keys_and_tagging {
            let response = signed_put_with_tagging(
                harness,
                key,
                format!("baseline for {key}").into_bytes(),
                tagging,
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::OK,
                "{} baseline PUT for {key}",
                matrix_state.label()
            );
        }
    };

    if matrix_state == PublicationMatrixState::Suspended {
        publish_baselines.await;
        let response = signed_put_bucket_versioning(harness, "Suspended").await;
        assert_eq!(response.status(), StatusCode::OK, "suspend matrix bucket");
        for (key, tagging) in keys_and_tagging {
            let response = signed_put_with_tagging(
                harness,
                key,
                format!("null baseline for {key}").into_bytes(),
                tagging,
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::OK,
                "suspended null baseline PUT for {key}"
            );
            assert_eq!(response.headers()["x-amz-version-id"], "null");
        }
    } else {
        publish_baselines.await;
    }

    let mut before = Vec::with_capacity(keys_and_tagging.len());
    for (key, _) in keys_and_tagging {
        let rows = publication_matrix_rows(state, harness.bucket(), key).await;
        let expected_rows = match matrix_state {
            PublicationMatrixState::Unversioned | PublicationMatrixState::Enabled => 1,
            PublicationMatrixState::Suspended => 2,
        };
        assert_eq!(
            rows.len(),
            expected_rows,
            "{} baseline index rows for {key}",
            matrix_state.label()
        );
        assert_eq!(rows.iter().filter(|row| row.is_latest).count(), 1);
        match matrix_state {
            PublicationMatrixState::Unversioned => assert!(rows[0].version_id.is_none()),
            PublicationMatrixState::Enabled => {
                uuid::Uuid::parse_str(rows[0].version_id.as_deref().unwrap())
                    .expect("enabled baseline public UUID");
            }
            PublicationMatrixState::Suspended => {
                uuid::Uuid::parse_str(rows[0].version_id.as_deref().unwrap())
                    .expect("suspended retained opaque public UUID");
                assert!(rows[1].version_id.is_none());
            }
        }
        before.push(PublicationMatrixBefore {
            key: (*key).to_owned(),
            rows,
        });
    }
    before
}

async fn object_fixture_tag(state: &Arc<AppState>, object_id: &str) -> Option<String> {
    store::entities::object_tag::Entity::find()
        .filter(store::entities::object_tag::Column::ObjectId.eq(object_id))
        .filter(store::entities::object_tag::Column::Key.eq("fixture"))
        .one(state.store.db())
        .await
        .expect("load fixture object tag")
        .map(|tag| tag.value)
}

#[allow(clippy::too_many_arguments)]
async fn assert_publication_matrix_object(
    harness: &impl S3TestEndpoint,
    state: &Arc<AppState>,
    matrix_state: PublicationMatrixState,
    before: &PublicationMatrixBefore,
    expected_cid: &str,
    expected_multipart: bool,
    expected_encrypted: bool,
    expected_fixture_tag: Option<&str>,
    version_surface: PublicationVersionSurface,
) -> String {
    let rows = publication_matrix_rows(state, harness.bucket(), &before.key).await;
    let expected_rows = match matrix_state {
        PublicationMatrixState::Unversioned => 1,
        PublicationMatrixState::Enabled | PublicationMatrixState::Suspended => 2,
    };
    assert_eq!(
        rows.len(),
        expected_rows,
        "{} post-publication index rows for {}",
        matrix_state.label(),
        before.key
    );
    assert_eq!(rows.iter().filter(|row| row.is_latest).count(), 1);
    assert_eq!(
        rows.iter().filter(|row| row.version_id.is_none()).count(),
        usize::from(matrix_state != PublicationMatrixState::Enabled),
        "{} null-slot count for {}",
        matrix_state.label(),
        before.key
    );

    let current = rows
        .iter()
        .find(|row| row.is_latest)
        .expect("one latest publication row");
    assert_eq!(current.kind, "object");
    assert_eq!(
        current.sequence,
        before
            .rows
            .iter()
            .map(|row| row.sequence)
            .max()
            .expect("baseline sequence")
            + 1
    );
    let object_id = current
        .object_id
        .as_deref()
        .expect("content version internal owner ID");
    uuid::Uuid::parse_str(object_id).expect("internal object UUID");
    let public_version_id = current.version_id.as_deref().unwrap_or("null");
    if let Some(opaque) = current.version_id.as_deref() {
        uuid::Uuid::parse_str(opaque).expect("opaque public version UUID");
    }
    assert_ne!(public_version_id, object_id);

    match version_surface {
        PublicationVersionSurface::Header(actual) => match matrix_state {
            PublicationMatrixState::Unversioned => assert_eq!(actual, None),
            PublicationMatrixState::Enabled | PublicationMatrixState::Suspended => {
                assert_eq!(actual.as_deref(), Some(public_version_id));
            }
        },
        PublicationVersionSurface::DiscoverOnly => {}
    }

    let object = store::object::get_by_id(state.store.db(), object_id)
        .await
        .expect("load matrix publication object");
    assert_eq!(object.bucket, harness.bucket());
    assert_eq!(object.key, before.key);
    assert_eq!(object.cid, expected_cid);
    assert_eq!(object.etag, expected_cid);
    assert_eq!(object.multipart, expected_multipart);
    assert_eq!(object.encrypted, expected_encrypted);
    assert_eq!(object.key_wrap.is_some(), expected_encrypted);
    assert!(object.is_latest);
    assert_eq!(
        object_fixture_tag(state, object_id).await.as_deref(),
        expected_fixture_tag
    );

    let projected = store::object::get_latest(state.store.db(), harness.bucket(), &before.key)
        .await
        .expect("ordinary current projection");
    assert_eq!(projected.id, object_id);
    assert_eq!(projected.cid, expected_cid);

    let current_head = signed_head(harness, &before.key, None).await;
    assert_eq!(current_head.status(), StatusCode::OK);
    assert_eq!(
        current_head.headers()[http::header::ETAG]
            .to_str()
            .unwrap()
            .trim_matches('"'),
        expected_cid
    );
    match matrix_state {
        PublicationMatrixState::Unversioned => {
            assert!(current_head.headers().get("x-amz-version-id").is_none())
        }
        PublicationMatrixState::Enabled | PublicationMatrixState::Suspended => {
            assert_eq!(
                current_head.headers()["x-amz-version-id"],
                public_version_id
            );
            let exact = signed_head_version(harness, &before.key, public_version_id).await;
            assert_eq!(exact.status(), StatusCode::OK);
            assert_eq!(exact.headers()["x-amz-version-id"], public_version_id);
            assert_eq!(
                exact.headers()[http::header::ETAG]
                    .to_str()
                    .unwrap()
                    .trim_matches('"'),
                expected_cid
            );
        }
    }

    let previous_object_ids = before
        .rows
        .iter()
        .map(|row| row.object_id.as_deref().expect("baseline content owner"))
        .collect::<Vec<_>>();
    for previous_id in &previous_object_ids {
        let previous = store::object::get_by_id(state.store.db(), previous_id)
            .await
            .expect("retained immutable baseline object");
        assert!(!previous.is_latest);
    }
    match matrix_state {
        PublicationMatrixState::Unversioned => {
            assert_eq!(
                object_fixture_tag(state, previous_object_ids[0]).await,
                None
            );
            assert!(
                rows.iter()
                    .all(|row| row.object_id.as_deref() != Some(previous_object_ids[0]))
            );
        }
        PublicationMatrixState::Enabled => {
            assert_eq!(
                object_fixture_tag(state, previous_object_ids[0])
                    .await
                    .as_deref(),
                Some("baseline")
            );
            assert!(
                rows.iter()
                    .any(|row| row.object_id.as_deref() == Some(previous_object_ids[0]))
            );
        }
        PublicationMatrixState::Suspended => {
            assert_eq!(
                object_fixture_tag(state, previous_object_ids[0])
                    .await
                    .as_deref(),
                Some("baseline")
            );
            assert_eq!(
                object_fixture_tag(state, previous_object_ids[1]).await,
                None
            );
            assert!(
                rows.iter()
                    .any(|row| row.object_id.as_deref() == Some(previous_object_ids[0]))
            );
            assert!(
                rows.iter()
                    .all(|row| row.object_id.as_deref() != Some(previous_object_ids[1]))
            );
        }
    }

    object_id.to_owned()
}

async fn assert_publication_matrix_pinning(
    harness: &PinningHarness,
    matrix_state: PublicationMatrixState,
    before: &PublicationMatrixBefore,
    current_object_id: &str,
) {
    let current_leases = owner_leases(harness, current_object_id).await;
    assert_eq!(current_leases.len(), 1, "one current manual lease");
    let current = &current_leases[0];
    assert_eq!(current.owner_object_id, current_object_id);
    assert_eq!(current.state, "active");
    let targets = lease_targets(harness, &current.id).await;
    assert!(!targets.is_empty(), "current lease has provider targets");
    assert!(targets.iter().all(|target| target.lease_id == current.id));
    assert!(
        harness
            .pin_jobs()
            .await
            .iter()
            .any(|job| job.lease_id.as_deref() == Some(current.id.as_str()))
    );
    let usage = harness
        .provider_usage(&harness.provider_key("pinata-primary"))
        .await;
    assert!(usage.reserved_pins > 0);
    assert!(usage.reserved_bytes > 0);

    let previous_ids = before
        .rows
        .iter()
        .map(|row| row.object_id.as_deref().expect("baseline lease owner"))
        .collect::<Vec<_>>();
    match matrix_state {
        PublicationMatrixState::Unversioned => assert!(
            owner_leases(harness, previous_ids[0])
                .await
                .iter()
                .all(|lease| lease.state == "cancelled")
        ),
        PublicationMatrixState::Enabled => assert!(
            owner_leases(harness, previous_ids[0])
                .await
                .iter()
                .all(|lease| lease.state == "active")
        ),
        PublicationMatrixState::Suspended => {
            assert!(
                owner_leases(harness, previous_ids[0])
                    .await
                    .iter()
                    .all(|lease| lease.state == "active")
            );
            assert!(
                owner_leases(harness, previous_ids[1])
                    .await
                    .iter()
                    .all(|lease| lease.state == "cancelled")
            );
        }
    }
}

async fn assert_public_ids_are_external(state: &Arc<AppState>, bucket: &str) {
    let rows = store::entities::object_version::Entity::find()
        .filter(store::entities::object_version::Column::Bucket.eq(bucket))
        .all(state.store.db())
        .await
        .expect("load all matrix version rows");
    let public_ids = rows
        .iter()
        .filter_map(|row| row.version_id.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        public_ids.iter().collect::<BTreeSet<_>>().len(),
        public_ids.len(),
        "every opaque public version ID is distinct"
    );
    let internal_ids = store::entities::object::Entity::find()
        .filter(store::entities::object::Column::Bucket.eq(bucket))
        .all(state.store.db())
        .await
        .expect("load all matrix internal objects")
        .into_iter()
        .map(|object| object.id)
        .collect::<BTreeSet<_>>();
    assert!(
        public_ids
            .iter()
            .all(|public_id| !internal_ids.contains(public_id)),
        "public version IDs never alias internal ownership IDs"
    );
}

async fn assert_no_remote_publication_rows(state: &Arc<AppState>) {
    assert_eq!(
        store::entities::pin_lease::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store::entities::pin_lease_target::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store::entities::pin_job::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store::entities::pin_provider_usage::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store::entities::remote_pin::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
}

async fn signed_post_import_matrix(
    harness: &impl S3TestEndpoint,
    key: &str,
    query: &[(&str, &str)],
    xml: String,
    tagging: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    headers.insert(
        "x-amz-tagging",
        HeaderValue::from_str(tagging).expect("matrix import tagging header"),
    );
    send_sigv4(
        reqwest::Method::POST,
        harness.endpoint(),
        harness.bucket(),
        key,
        query,
        xml.into_bytes(),
        headers,
        "test",
    )
    .await
}

fn matrix_pinning_config(
    add_replies: Vec<AddReply>,
    cat_bodies: HashMap<String, Vec<u8>>,
) -> PinningHarnessConfig {
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = KuboScript {
        add_replies,
        cat_bodies,
    };
    config.pinata_script.clear();
    config
}

const MATRIX_BASELINE_PIN_TAGS: &str = "fixture=baseline&ipfs-s3%3Apin=true";

const MATRIX_ACTUAL_PIN_TAGS: &str = "fixture=actual&ipfs-s3%3Apin=true";

const MATRIX_ZIP_PIN_TAGS: &str =
    "fixture=actual&ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn versioning_publication_path_put() {
    let config = matrix_pinning_config(
        [
            "QmPutUBase",
            "QmPutU",
            "QmPutEBase",
            "QmPutE",
            "QmPutSOpaque",
            "QmPutSNull",
            "QmPutS",
        ]
        .into_iter()
        .map(AddReply::Ok)
        .collect(),
        HashMap::new(),
    );
    let harness = start_pinning_harness(config).await;
    for (matrix_state, expected_cid) in [
        (PublicationMatrixState::Unversioned, "QmPutU"),
        (PublicationMatrixState::Enabled, "QmPutE"),
        (PublicationMatrixState::Suspended, "QmPutS"),
    ] {
        let key = format!("matrix/{}/put.bin", matrix_state.label());
        let before = prepare_publication_matrix_state(
            &harness,
            &harness.state,
            matrix_state,
            &[(&key, MATRIX_BASELINE_PIN_TAGS)],
        )
        .await;
        let response = signed_put_with_tagging(
            &harness,
            &key,
            format!("actual {} PUT", matrix_state.label()).into_bytes(),
            MATRIX_ACTUAL_PIN_TAGS,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[http::header::ETAG]
                .to_str()
                .unwrap()
                .trim_matches('"'),
            expected_cid
        );
        let surface = response_version_surface(&response);
        let owner = assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[0],
            expected_cid,
            false,
            false,
            Some("actual"),
            surface,
        )
        .await;
        assert_publication_matrix_pinning(&harness, matrix_state, &before[0], &owner).await;
    }
    assert_public_ids_are_external(&harness.state, &harness.bucket).await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn versioning_publication_path_copy() {
    let config = matrix_pinning_config(
        [
            "QmCopyUBase",
            "QmCopyUSource",
            "QmCopyEBase",
            "QmCopyESource",
            "QmCopySOpaque",
            "QmCopySNull",
            "QmCopySSource",
        ]
        .into_iter()
        .map(AddReply::Ok)
        .collect(),
        HashMap::new(),
    );
    let harness = start_pinning_harness(config).await;
    for (matrix_state, expected_cid) in [
        (PublicationMatrixState::Unversioned, "QmCopyUSource"),
        (PublicationMatrixState::Enabled, "QmCopyESource"),
        (PublicationMatrixState::Suspended, "QmCopySSource"),
    ] {
        let destination = format!("matrix/{}/copy.bin", matrix_state.label());
        let source = format!("matrix/{}/copy-source.bin", matrix_state.label());
        let before = prepare_publication_matrix_state(
            &harness,
            &harness.state,
            matrix_state,
            &[(&destination, MATRIX_BASELINE_PIN_TAGS)],
        )
        .await;
        let source_put = signed_put(
            &harness,
            &source,
            &[],
            format!("copy source {}", matrix_state.label()).into_bytes(),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(source_put.status(), StatusCode::OK);
        let response =
            signed_copy_with_tagging(&harness, &source, &destination, MATRIX_ACTUAL_PIN_TAGS).await;
        assert_eq!(response.status(), StatusCode::OK);
        let surface = response_version_surface(&response);
        let owner = assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[0],
            expected_cid,
            false,
            false,
            Some("actual"),
            surface,
        )
        .await;
        assert_publication_matrix_pinning(&harness, matrix_state, &before[0], &owner).await;
    }
    assert_public_ids_are_external(&harness.state, &harness.bucket).await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn versioning_publication_path_multipart() {
    let mut config = matrix_pinning_config(
        [
            "QmMultipartUBase",
            "QmMultipartUPart",
            "QmMultipartURoot",
            "QmMultipartEBase",
            "QmMultipartEPart",
            "QmMultipartERoot",
            "QmMultipartSOpaque",
            "QmMultipartSNull",
            "QmMultipartSPart",
            "QmMultipartSRoot",
        ]
        .into_iter()
        .map(AddReply::Ok)
        .collect(),
        HashMap::from([
            ("QmMultipartUPart".to_owned(), b"multipart U".to_vec()),
            ("QmMultipartEPart".to_owned(), b"multipart E".to_vec()),
            ("QmMultipartSPart".to_owned(), b"multipart S".to_vec()),
        ]),
    );
    config.explicit_identity = true;
    let harness = start_pinning_harness(config).await;
    for (matrix_state, part_body, expected_cid) in [
        (
            PublicationMatrixState::Unversioned,
            b"multipart U".as_slice(),
            "QmMultipartURoot",
        ),
        (
            PublicationMatrixState::Enabled,
            b"multipart E".as_slice(),
            "QmMultipartERoot",
        ),
        (
            PublicationMatrixState::Suspended,
            b"multipart S".as_slice(),
            "QmMultipartSRoot",
        ),
    ] {
        let key = format!("matrix/{}/multipart.bin", matrix_state.label());
        let before = prepare_publication_matrix_state(
            &harness,
            &harness.state,
            matrix_state,
            &[(&key, MATRIX_BASELINE_PIN_TAGS)],
        )
        .await;
        let create =
            signed_create_multipart_upload_with_tagging(&harness, &key, MATRIX_ACTUAL_PIN_TAGS)
                .await;
        assert_eq!(create.status(), StatusCode::OK);
        let upload_id = xml_element_text(&create.text().await.unwrap(), "UploadId");
        let part = signed_upload_part(&harness, &key, &upload_id, 1, part_body.to_vec()).await;
        assert_eq!(part.status(), StatusCode::OK);
        let etag = part.headers()[http::header::ETAG]
            .to_str()
            .unwrap()
            .trim_matches('"')
            .to_owned();
        let response = signed_complete_multipart(&harness, &key, &upload_id, 1, &etag).await;
        assert_eq!(response.status(), StatusCode::OK);
        let surface = response_version_surface(&response);
        let owner = assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[0],
            expected_cid,
            true,
            false,
            Some("actual"),
            surface,
        )
        .await;
        assert_publication_matrix_pinning(&harness, matrix_state, &before[0], &owner).await;
        assert!(
            store::multipart::get_upload(harness.state.store.db(), &upload_id)
                .await
                .is_err()
        );
    }
    assert_public_ids_are_external(&harness.state, &harness.bucket).await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn versioning_publication_path_direct_cid_import() {
    let config = ImportHarnessConfig {
        kubo_script: KuboScript {
            add_replies: [
                "QmCidImportUBase",
                "QmCidImportEBase",
                "QmCidImportSOpaque",
                "QmCidImportSNull",
            ]
            .into_iter()
            .map(AddReply::Ok)
            .collect(),
            cat_bodies: HashMap::from([(
                IMPORT_CID.to_owned(),
                b"matrix CID import body".to_vec(),
            )]),
        },
        ..Default::default()
    };
    let harness = start_import_harness(config).await;
    for matrix_state in [
        PublicationMatrixState::Unversioned,
        PublicationMatrixState::Enabled,
        PublicationMatrixState::Suspended,
    ] {
        let key = format!("matrix/{}/cid-import.bin", matrix_state.label());
        let before = prepare_publication_matrix_state(
            &harness,
            &harness.state,
            matrix_state,
            &[(&key, "fixture=baseline")],
        )
        .await;
        let response = signed_post_import_matrix(
            &harness,
            &key,
            &[("ipfs3-import", "")],
            format!("<IPFS3ImportRequest><CID>{IMPORT_CID}</CID></IPFS3ImportRequest>"),
            "fixture=actual",
        )
        .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(response.headers().get("x-amz-version-id").is_none());
        let job_id = response.headers()["x-ipfs3-import-job-id"]
            .to_str()
            .unwrap()
            .to_owned();
        let job = wait_for_import_state(&harness, &job_id, &["completed"]).await;
        assert_eq!(job.final_cid.as_deref(), Some(IMPORT_CID));
        assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[0],
            IMPORT_CID,
            false,
            false,
            Some("actual"),
            PublicationVersionSurface::DiscoverOnly,
        )
        .await;
        assert_eq!(import_result_count(&harness, &job_id).await, 1);
        assert_no_remote_publication_rows(&harness.state).await;
    }
    assert_public_ids_are_external(&harness.state, &harness.bucket).await;
    assert!(harness.kubo_args("/api/v0/pin/rm").await.is_empty());
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn versioning_publication_path_direct_https_import() {
    let config = ImportHarnessConfig {
        kubo_script: KuboScript {
            add_replies: [
                "QmHttpsUBase",
                IMPORT_TEST_CID_V0,
                "QmHttpsEBase",
                IMPORT_TEST_CID_V0,
                "QmHttpsSOpaque",
                "QmHttpsSNull",
                IMPORT_TEST_CID_V0,
            ]
            .into_iter()
            .map(AddReply::Ok)
            .collect(),
            cat_bodies: HashMap::from([(
                IMPORT_TEST_CID_V0.to_owned(),
                b"HTTPS matrix body".to_vec(),
            )]),
        },
        ..Default::default()
    };
    let harness = start_import_harness(config).await;
    for (matrix_state, path, body, expected_cid) in [
        (
            PublicationMatrixState::Unversioned,
            "/matrix-https-u",
            b"HTTPS matrix body".as_slice(),
            IMPORT_TEST_CID_V0,
        ),
        (
            PublicationMatrixState::Enabled,
            "/matrix-https-e",
            b"HTTPS matrix body".as_slice(),
            IMPORT_TEST_CID_V0,
        ),
        (
            PublicationMatrixState::Suspended,
            "/matrix-https-s",
            b"HTTPS matrix body".as_slice(),
            IMPORT_TEST_CID_V0,
        ),
    ] {
        let key = format!("matrix/{}/https-import.bin", matrix_state.label());
        let before = prepare_publication_matrix_state(
            &harness,
            &harness.state,
            matrix_state,
            &[(&key, "fixture=baseline")],
        )
        .await;
        harness
            .source
            .set_reply(path, TestHttpsReply::chunked(body.to_vec()));
        let response = signed_post_import_matrix(
            &harness,
            &key,
            &[("ipfs3-import", "")],
            import_url_xml(&harness.source.url(path)),
            "fixture=actual",
        )
        .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(response.headers().get("x-amz-version-id").is_none());
        let job_id = response.headers()["x-ipfs3-import-job-id"]
            .to_str()
            .unwrap()
            .to_owned();
        wait_for_import_state(&harness, &job_id, &["completed"]).await;
        assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[0],
            expected_cid,
            false,
            false,
            Some("actual"),
            PublicationVersionSurface::DiscoverOnly,
        )
        .await;
        assert_eq!(import_result_count(&harness, &job_id).await, 1);
        assert_no_remote_publication_rows(&harness.state).await;
    }
    assert_public_ids_are_external(&harness.state, &harness.bucket).await;
    assert!(harness.kubo_args("/api/v0/pin/rm").await.is_empty());
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn versioning_publication_path_import_zip() {
    let archive = legal_single_entry_zip();
    let add_cids = [
        "QmImportZipUArchiveBase",
        "QmImportZipUEntryBase",
        IMPORT_TEST_CID_V0,
        IMPORT_CID,
        "QmImportZipEArchiveBase",
        "QmImportZipEEntryBase",
        IMPORT_TEST_CID_V0,
        IMPORT_CID,
        "QmImportZipSOpaqueArchive",
        "QmImportZipSOpaqueEntry",
        "QmImportZipSNullArchive",
        "QmImportZipSNullEntry",
        IMPORT_TEST_CID_V0,
        IMPORT_CID,
    ];
    let config = ImportHarnessConfig {
        kubo_script: KuboScript {
            add_replies: add_cids.into_iter().map(AddReply::Ok).collect(),
            cat_bodies: HashMap::from([
                (IMPORT_TEST_CID_V0.to_owned(), archive.clone()),
                (IMPORT_CID.to_owned(), SINGLE_ENTRY_BYTES.to_vec()),
            ]),
        },
        ..Default::default()
    };
    let harness = start_import_harness(config).await;
    for (matrix_state, path, archive_cid, entry_cid) in [
        (
            PublicationMatrixState::Unversioned,
            "/matrix-import-zip-u",
            IMPORT_TEST_CID_V0,
            IMPORT_CID,
        ),
        (
            PublicationMatrixState::Enabled,
            "/matrix-import-zip-e",
            IMPORT_TEST_CID_V0,
            IMPORT_CID,
        ),
        (
            PublicationMatrixState::Suspended,
            "/matrix-import-zip-s",
            IMPORT_TEST_CID_V0,
            IMPORT_CID,
        ),
    ] {
        let archive_key = format!("matrix/{}/import.zip", matrix_state.label());
        let prefix = format!("matrix/{}/import-output/", matrix_state.label());
        let entry_key = format!("{prefix}file.txt");
        let before = prepare_publication_matrix_state(
            &harness,
            &harness.state,
            matrix_state,
            &[
                (&archive_key, "fixture=baseline"),
                (&entry_key, "fixture=baseline"),
            ],
        )
        .await;
        assert_no_remote_publication_rows(&harness.state).await;
        harness
            .source
            .set_reply(path, TestHttpsReply::chunked(archive.clone()));
        let response = signed_post_import_matrix(
            &harness,
            &archive_key,
            &[("ipfs3-import", ""), ("decompress-zip", &prefix)],
            import_url_xml(&harness.source.url(path)),
            "fixture=actual",
        )
        .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(response.headers().get("x-amz-version-id").is_none());
        let job_id = response.headers()["x-ipfs3-import-job-id"]
            .to_str()
            .unwrap()
            .to_owned();
        let job = wait_for_import_state(&harness, &job_id, &["completed"]).await;
        assert_eq!(job.entries_succeeded, 1);
        assert_eq!(import_result_count(&harness, &job_id).await, 2);
        assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[0],
            archive_cid,
            false,
            false,
            Some("actual"),
            PublicationVersionSurface::DiscoverOnly,
        )
        .await;
        assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[1],
            entry_cid,
            false,
            false,
            None,
            PublicationVersionSurface::DiscoverOnly,
        )
        .await;
        assert_no_remote_publication_rows(&harness.state).await;
    }
    assert_public_ids_are_external(&harness.state, &harness.bucket).await;
    assert!(harness.kubo_args("/api/v0/pin/rm").await.is_empty());
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn versioning_publication_path_direct_zip() {
    let archive = legal_single_entry_zip();
    let add_cids = [
        "QmDirectZipUArchiveBase",
        "QmDirectZipUEntryBase",
        "QmDirectZipUArchive",
        "QmDirectZipUEntry",
        "QmDirectZipEArchiveBase",
        "QmDirectZipEEntryBase",
        "QmDirectZipEArchive",
        "QmDirectZipEEntry",
        "QmDirectZipSOpaqueArchive",
        "QmDirectZipSOpaqueEntry",
        "QmDirectZipSNullArchive",
        "QmDirectZipSNullEntry",
        "QmDirectZipSArchive",
        "QmDirectZipSEntry",
    ];
    let mut config = matrix_pinning_config(
        add_cids.into_iter().map(AddReply::Ok).collect(),
        HashMap::from([
            ("QmDirectZipUArchive".to_owned(), archive.clone()),
            ("QmDirectZipEArchive".to_owned(), archive.clone()),
            ("QmDirectZipSArchive".to_owned(), archive.clone()),
        ]),
    );
    config.explicit_identity = true;
    let harness = start_pinning_harness(config).await;
    for (matrix_state, archive_cid, entry_cid) in [
        (
            PublicationMatrixState::Unversioned,
            "QmDirectZipUArchive",
            "QmDirectZipUEntry",
        ),
        (
            PublicationMatrixState::Enabled,
            "QmDirectZipEArchive",
            "QmDirectZipEEntry",
        ),
        (
            PublicationMatrixState::Suspended,
            "QmDirectZipSArchive",
            "QmDirectZipSEntry",
        ),
    ] {
        let archive_key = format!("matrix/{}/direct.zip", matrix_state.label());
        let prefix = format!("matrix/{}/direct-output/", matrix_state.label());
        let entry_key = format!("{prefix}file.txt");
        let before = prepare_publication_matrix_state(
            &harness,
            &harness.state,
            matrix_state,
            &[
                (&archive_key, MATRIX_BASELINE_PIN_TAGS),
                (&entry_key, "fixture=baseline"),
            ],
        )
        .await;
        let response = signed_decompress_zip_put(
            &harness,
            &archive_key,
            &prefix,
            archive.clone(),
            MATRIX_ZIP_PIN_TAGS,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let surface = response_version_surface(&response);
        let owner = assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[0],
            archive_cid,
            false,
            false,
            Some("actual"),
            surface,
        )
        .await;
        assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[1],
            entry_cid,
            false,
            false,
            None,
            PublicationVersionSurface::DiscoverOnly,
        )
        .await;
        assert_publication_matrix_pinning(&harness, matrix_state, &before[0], &owner).await;
    }
    assert_public_ids_are_external(&harness.state, &harness.bucket).await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn versioning_publication_path_multipart_zip() {
    let archive = legal_single_entry_zip();
    let add_cids = [
        "QmMultipartZipUArchiveBase",
        "QmMultipartZipUEntryBase",
        "QmMultipartZipUPart",
        "QmMultipartZipURoot",
        "QmMultipartZipUEntry",
        "QmMultipartZipEArchiveBase",
        "QmMultipartZipEEntryBase",
        "QmMultipartZipEPart",
        "QmMultipartZipERoot",
        "QmMultipartZipEEntry",
        "QmMultipartZipSOpaqueArchive",
        "QmMultipartZipSOpaqueEntry",
        "QmMultipartZipSNullArchive",
        "QmMultipartZipSNullEntry",
        "QmMultipartZipSPart",
        "QmMultipartZipSRoot",
        "QmMultipartZipSEntry",
    ];
    let mut config = matrix_pinning_config(
        add_cids.into_iter().map(AddReply::Ok).collect(),
        HashMap::from([
            ("QmMultipartZipUPart".to_owned(), archive.clone()),
            ("QmMultipartZipURoot".to_owned(), archive.clone()),
            ("QmMultipartZipEPart".to_owned(), archive.clone()),
            ("QmMultipartZipERoot".to_owned(), archive.clone()),
            ("QmMultipartZipSPart".to_owned(), archive.clone()),
            ("QmMultipartZipSRoot".to_owned(), archive.clone()),
        ]),
    );
    config.explicit_identity = true;
    let harness = start_pinning_harness(config).await;
    for (matrix_state, root_cid, entry_cid) in [
        (
            PublicationMatrixState::Unversioned,
            "QmMultipartZipURoot",
            "QmMultipartZipUEntry",
        ),
        (
            PublicationMatrixState::Enabled,
            "QmMultipartZipERoot",
            "QmMultipartZipEEntry",
        ),
        (
            PublicationMatrixState::Suspended,
            "QmMultipartZipSRoot",
            "QmMultipartZipSEntry",
        ),
    ] {
        let archive_key = format!("matrix/{}/multipart.zip", matrix_state.label());
        let prefix = format!("matrix/{}/multipart-output/", matrix_state.label());
        let entry_key = format!("{prefix}file.txt");
        let before = prepare_publication_matrix_state(
            &harness,
            &harness.state,
            matrix_state,
            &[
                (&archive_key, MATRIX_BASELINE_PIN_TAGS),
                (&entry_key, "fixture=baseline"),
            ],
        )
        .await;
        let create = signed_create_multipart_zip_upload_with_tagging(
            &harness,
            &archive_key,
            &prefix,
            MATRIX_ZIP_PIN_TAGS,
        )
        .await;
        assert_eq!(create.status(), StatusCode::OK);
        let upload_id = xml_element_text(&create.text().await.unwrap(), "UploadId");
        let part = signed_upload_part(&harness, &archive_key, &upload_id, 1, archive.clone()).await;
        assert_eq!(part.status(), StatusCode::OK);
        let etag = part.headers()[http::header::ETAG]
            .to_str()
            .unwrap()
            .trim_matches('"')
            .to_owned();
        let response =
            signed_complete_multipart(&harness, &archive_key, &upload_id, 1, &etag).await;
        assert_eq!(response.status(), StatusCode::OK);
        let surface = response_version_surface(&response);
        let owner = assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[0],
            root_cid,
            true,
            false,
            Some("actual"),
            surface,
        )
        .await;
        assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[1],
            entry_cid,
            false,
            false,
            None,
            PublicationVersionSurface::DiscoverOnly,
        )
        .await;
        assert_publication_matrix_pinning(&harness, matrix_state, &before[0], &owner).await;
        assert!(
            store::multipart::get_upload(harness.state.store.db(), &upload_id)
                .await
                .is_err()
        );
    }
    assert_public_ids_are_external(&harness.state, &harness.bucket).await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

async fn signed_delete_object_versions(
    harness: &impl S3TestEndpoint,
    objects: &[(&str, Option<&str>)],
    quiet: bool,
) -> reqwest::Response {
    let body = delete_identifiers_xml(objects, quiet);
    send_sigv4(
        reqwest::Method::POST,
        harness.endpoint(),
        harness.bucket(),
        "",
        &[("delete", "")],
        body.clone(),
        delete_headers(&body),
        "test",
    )
    .await
}
