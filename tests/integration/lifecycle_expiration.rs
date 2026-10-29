use super::common::{
    OwnedTestEndpoint, assert_mpu_s3_error, assert_s3_error, assert_signed_body, scripted,
    signed_delete_bucket_lifecycle_configuration,
    signed_delete_bucket_lifecycle_configuration_with_headers, signed_delete_object,
    signed_delete_object_version, signed_get, signed_get_bucket_lifecycle_configuration,
    signed_get_bucket_lifecycle_configuration_with_headers, signed_get_version,
    signed_list_object_versions, signed_list_objects, signed_put,
    signed_put_bucket_lifecycle_configuration_xml,
    signed_put_bucket_lifecycle_configuration_xml_with_headers, signed_put_bucket_versioning,
    signed_put_object_tagging, signed_put_with_tagging, sse_c_headers,
    stored_lifecycle_configuration_for, xml_element_values,
};
use crate::support;
use crate::support::decompress::{KuboBlockTarget, KuboScript, TestHarness, start_harness};
use crate::support::lifecycle::{LifecycleHarness, start_lifecycle_harness};
use chrono::Duration as ChronoDuration;
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::store;
use sea_orm::{ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter, Set};
use std::collections::HashMap;

fn lifecycle_configuration_xml(rule_id: &str) -> String {
    format!(
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ID>{rule_id}</ID><Status>Enabled</Status><Filter><Prefix>logs/</Prefix></Filter>\
         <Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>"
    )
}

fn lifecycle_current_expiration_xml(rule_id: &str, expiration: &str) -> String {
    format!(
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ID>{rule_id}</ID><Status>Enabled</Status><Filter/>{expiration}</Rule>\
         </LifecycleConfiguration>"
    )
}

fn lifecycle_noncurrent_expiration_xml(
    rule_id: &str,
    newer_noncurrent_versions: Option<u32>,
) -> String {
    let newer = newer_noncurrent_versions.map_or(String::new(), |count| {
        format!("<NewerNoncurrentVersions>{count}</NewerNoncurrentVersions>")
    });
    format!(
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ID>{rule_id}</ID><Status>Enabled</Status><Filter/>\
         <NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays>{newer}\
         </NoncurrentVersionExpiration></Rule></LifecycleConfiguration>"
    )
}

fn expected_bucket_owner_headers(owner: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-expected-bucket-owner",
        HeaderValue::from_str(owner).expect("valid expected owner header"),
    );
    headers
}

async fn schedule_lifecycle_action(
    harness: &LifecycleHarness,
    configuration: String,
) -> ipfs_s3_gateway::lifecycle::model::ClaimedLifecycleAction {
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(harness, configuration)
            .await
            .status(),
        StatusCode::OK,
        "lifecycle acceptance configuration must save"
    );
    harness.run_one_scan_page().await;
    harness.claim_one_action().await
}

async fn stored_lifecycle_configuration(
    harness: &TestHarness,
) -> ipfs_s3_gateway::store::entities::bucket_lifecycle_config::Model {
    ipfs_s3_gateway::store::entities::bucket_lifecycle_config::Entity::find_by_id(
        harness.bucket.clone(),
    )
    .one(harness.state.store.db())
    .await
    .expect("read stored lifecycle configuration")
    .expect("stored lifecycle configuration")
}

#[tokio::test]
async fn lifecycle_malformed_xml_uses_framework_error_without_write() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let saved = signed_put_bucket_lifecycle_configuration_xml(
        &harness,
        lifecycle_configuration_xml("initial"),
    )
    .await;
    assert_eq!(
        saved.status(),
        StatusCode::OK,
        "save valid lifecycle configuration"
    );
    let before = stored_lifecycle_configuration(&harness).await;

    assert_s3_error(
        signed_put_bucket_lifecycle_configuration_xml(
            &harness,
            "<LifecycleConfiguration><Rule>".to_owned(),
        )
        .await,
        StatusCode::BAD_REQUEST,
        "MalformedXML",
        "",
    )
    .await;

    let after = stored_lifecycle_configuration(&harness).await;
    assert_eq!(after.canonical_json, before.canonical_json);
    assert_eq!(after.revision, before.revision);
}

#[tokio::test]
async fn lifecycle_nested_unknown_xml_uses_framework_error_without_write() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let saved = signed_put_bucket_lifecycle_configuration_xml(
        &harness,
        lifecycle_configuration_xml("initial"),
    )
    .await;
    assert_eq!(
        saved.status(),
        StatusCode::OK,
        "save valid lifecycle configuration"
    );
    let before = stored_lifecycle_configuration(&harness).await;

    assert_s3_error(
        signed_put_bucket_lifecycle_configuration_xml(
            &harness,
            "<LifecycleConfiguration><Rule><ID>invalid</ID><Status>Enabled</Status>\
             <Filter><UnexpectedNested/></Filter><Expiration><Days>30</Days></Expiration>\
             </Rule></LifecycleConfiguration>"
                .to_owned(),
        )
        .await,
        StatusCode::BAD_REQUEST,
        "MalformedXML",
        "",
    )
    .await;

    let after = stored_lifecycle_configuration(&harness).await;
    assert_eq!(after.canonical_json, before.canonical_json);
    assert_eq!(after.revision, before.revision);
}

#[tokio::test]
async fn lifecycle_root_unknown_element_is_ignored_before_typed_handler() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let response = signed_put_bucket_lifecycle_configuration_xml(
        &harness,
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ID>kept</ID><Status>Enabled</Status><Filter><Prefix>logs/</Prefix></Filter>\
         <Expiration><Days>30</Days></Expiration></Rule><UnexpectedRoot>discarded</UnexpectedRoot>\
         </LifecycleConfiguration>"
            .to_owned(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = signed_get_bucket_lifecycle_configuration(&harness).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("lifecycle GET XML");
    assert!(
        body.contains("<ID>kept</ID>"),
        "missing supported rule: {body}"
    );
    assert!(
        !body.contains("UnexpectedRoot"),
        "s3s must discard unknown direct root children before the typed handler: {body}"
    );
}

#[tokio::test]
async fn lifecycle_signed_configuration_put_get_delete_uses_s3s_operations() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let put = signed_put_bucket_lifecycle_configuration_xml(
        &harness,
        lifecycle_configuration_xml("signed"),
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let saved = stored_lifecycle_configuration(&harness).await;
    assert_eq!(saved.revision, 1);

    let get = signed_get_bucket_lifecycle_configuration(&harness).await;
    assert_eq!(get.status(), StatusCode::OK);
    let body = get.text().await.expect("lifecycle GET XML");
    assert!(
        body.contains("<ID>signed</ID>"),
        "missing signed rule: {body}"
    );
    assert!(
        !body.contains("Transition"),
        "unsupported action leaked: {body}"
    );

    let delete = signed_delete_bucket_lifecycle_configuration(&harness).await;
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);
    let tombstone = stored_lifecycle_configuration(&harness).await;
    assert_eq!(tombstone.revision, 2);
    assert_eq!(tombstone.canonical_json, None);
    assert_s3_error(
        signed_get_bucket_lifecycle_configuration(&harness).await,
        StatusCode::NOT_FOUND,
        "NoSuchLifecycleConfiguration",
        "lifecycle configuration not found",
    )
    .await;
}

#[tokio::test]
async fn lifecycle_signed_configuration() {
    let harness = start_lifecycle_harness(KuboScript {
        add_replies: Vec::new(),
        cat_bodies: HashMap::new(),
    })
    .await;

    let absent = signed_get_bucket_lifecycle_configuration(&harness).await;
    assert_eq!(absent.status(), StatusCode::NOT_FOUND);
    assert!(
        absent
            .text()
            .await
            .expect("read absent lifecycle error")
            .contains("NoSuchLifecycleConfiguration"),
        "absent lifecycle configuration must use the S3 error code"
    );

    let put = signed_put_bucket_lifecycle_configuration_xml_with_headers(
        &harness,
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ID>canonical</ID><Status>Enabled</Status><Filter><And><Prefix>logs/</Prefix>\
         <Tag><Key>class</Key><Value>archive</Value></Tag></And></Filter>\
         <Expiration><Days>3</Days></Expiration></Rule></LifecycleConfiguration>"
            .to_owned(),
        expected_bucket_owner_headers(&harness.owner),
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let first = stored_lifecycle_configuration_for(&harness).await;
    assert_eq!(first.revision, 1);

    let get = signed_get_bucket_lifecycle_configuration_with_headers(
        &harness,
        expected_bucket_owner_headers(&harness.owner),
    )
    .await;
    assert_eq!(get.status(), StatusCode::OK);
    let canonical_xml = get.text().await.expect("read canonical lifecycle XML");
    assert_eq!(xml_element_values(&canonical_xml, "ID"), vec!["canonical"]);
    assert_eq!(xml_element_values(&canonical_xml, "Prefix"), vec!["logs/"]);
    assert_eq!(xml_element_values(&canonical_xml, "Days"), vec!["3"]);
    assert!(!canonical_xml.contains("Transition"));

    let replacement = signed_put_bucket_lifecycle_configuration_xml(
        &harness,
        lifecycle_configuration_xml("replacement"),
    )
    .await;
    assert_eq!(replacement.status(), StatusCode::OK);
    let second = stored_lifecycle_configuration_for(&harness).await;
    assert_eq!(second.revision, 2);
    let replacement_get = signed_get_bucket_lifecycle_configuration(&harness).await;
    assert_eq!(replacement_get.status(), StatusCode::OK);
    let replacement_xml = replacement_get
        .text()
        .await
        .expect("read replacement lifecycle XML");
    assert_eq!(
        xml_element_values(&replacement_xml, "ID"),
        vec!["replacement"]
    );
    assert!(!replacement_xml.contains("canonical"));

    let wrong_owner = signed_get_bucket_lifecycle_configuration_with_headers(
        &harness,
        expected_bucket_owner_headers("different-owner"),
    )
    .await;
    assert_eq!(wrong_owner.status(), StatusCode::FORBIDDEN);
    assert!(
        wrong_owner
            .text()
            .await
            .expect("read owner mismatch error")
            .contains("AccessDenied"),
        "mismatched expected owner must be denied"
    );
    let missing = OwnedTestEndpoint {
        endpoint: harness.endpoint.clone(),
        bucket: "lifecycle-missing-bucket".to_owned(),
    };
    let missing_response = signed_put_bucket_lifecycle_configuration_xml(
        &missing,
        lifecycle_configuration_xml("missing"),
    )
    .await;
    assert_eq!(missing_response.status(), StatusCode::NOT_FOUND);
    assert!(
        missing_response
            .text()
            .await
            .expect("read missing bucket error")
            .contains("NoSuchBucket"),
        "missing bucket must retain the S3 error code"
    );

    for invalid_xml in [
        "<LifecycleConfiguration><Rule>".to_owned(),
        "<LifecycleConfiguration><Rule><ID>nested</ID><Status>Enabled</Status>\
         <Filter><UnexpectedNested/></Filter><Expiration><Days>3</Days></Expiration>\
         </Rule></LifecycleConfiguration>"
            .to_owned(),
    ] {
        let rejected = signed_put_bucket_lifecycle_configuration_xml(&harness, invalid_xml).await;
        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
        assert!(
            rejected
                .text()
                .await
                .expect("read framework XML rejection")
                .contains("MalformedXML"),
            "framework-invalid lifecycle XML must be rejected before write"
        );
        assert_eq!(stored_lifecycle_configuration_for(&harness).await, second);
    }

    let root_unknown = signed_put_bucket_lifecycle_configuration_xml(
        &harness,
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ID>root-kept</ID><Status>Enabled</Status><Filter/><Expiration><Days>3</Days>\
         </Expiration></Rule><IgnoredByFramework>ignored</IgnoredByFramework>\
         </LifecycleConfiguration>"
            .to_owned(),
    )
    .await;
    assert_eq!(root_unknown.status(), StatusCode::OK);
    let root_unknown_xml = signed_get_bucket_lifecycle_configuration(&harness)
        .await
        .text()
        .await
        .expect("read root-child lifecycle projection");
    assert_eq!(
        xml_element_values(&root_unknown_xml, "ID"),
        vec!["root-kept"]
    );
    assert!(!root_unknown_xml.contains("IgnoredByFramework"));
    let before_rejection = stored_lifecycle_configuration_for(&harness).await;

    for invalid_xml in [
        "<LifecycleConfiguration><Rule><ID>transition</ID><Status>Enabled</Status><Filter/>\
         <Expiration><Days>3</Days></Expiration><Transition><Days>1</Days>\
         <StorageClass>STANDARD_IA</StorageClass></Transition></Rule></LifecycleConfiguration>"
            .to_owned(),
        "<LifecycleConfiguration><Rule><ID>noncurrent-transition</ID><Status>Enabled</Status>\
         <Filter/><Expiration><Days>3</Days></Expiration><NoncurrentVersionTransition>\
         <NoncurrentDays>1</NoncurrentDays><StorageClass>STANDARD_IA</StorageClass>\
         </NoncurrentVersionTransition></Rule></LifecycleConfiguration>"
            .to_owned(),
        "<LifecycleConfiguration><Rule><ID>abort-zero</ID><Status>Enabled</Status><Filter/>\
         <Expiration><Days>3</Days></Expiration><AbortIncompleteMultipartUpload>\
         <DaysAfterInitiation>0</DaysAfterInitiation></AbortIncompleteMultipartUpload>\
         </Rule></LifecycleConfiguration>"
            .to_owned(),
        "<LifecycleConfiguration><Rule><ID>conflict</ID><Status>Enabled</Status><Filter/>\
         <Expiration><Days>3</Days><Date>2020-01-01T00:00:00Z</Date></Expiration>\
         </Rule></LifecycleConfiguration>"
            .to_owned(),
        "<LifecycleConfiguration><Rule><ID>newer-without-filter</ID><Status>Enabled</Status>\
         <Prefix>logs/</Prefix><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays>\
         <NewerNoncurrentVersions>1</NewerNoncurrentVersions></NoncurrentVersionExpiration>\
         </Rule></LifecycleConfiguration>"
            .to_owned(),
    ] {
        let rejected = signed_put_bucket_lifecycle_configuration_xml(&harness, invalid_xml).await;
        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
        assert!(
            rejected
                .text()
                .await
                .expect("read typed lifecycle rejection")
                .contains("InvalidRequest"),
            "unsupported or contradictory lifecycle configuration must be rejected"
        );
        assert_eq!(
            stored_lifecycle_configuration_for(&harness).await,
            before_rejection,
            "rejected lifecycle configuration must not advance its revision"
        );
    }

    let deleted = signed_delete_bucket_lifecycle_configuration_with_headers(
        &harness,
        expected_bucket_owner_headers(&harness.owner),
    )
    .await;
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    let absent_after_delete = signed_get_bucket_lifecycle_configuration(&harness).await;
    assert_eq!(absent_after_delete.status(), StatusCode::NOT_FOUND);
    harness.shutdown().await;
}

#[tokio::test]
async fn lifecycle_expiration_current_unversioned() {
    let mut unversioned = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleCurrentUnversioned",
        1,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put(
            &unversioned,
            "unversioned-current",
            &[],
            b"unversioned".to_vec(),
            HeaderMap::new(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &unversioned,
            lifecycle_current_expiration_xml(
                "unversioned-expire",
                "<Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration>",
            ),
        )
        .await
        .status(),
        StatusCode::OK
    );
    unversioned.run_one_scan_page().await;
    let claim = unversioned.claim_one_action().await;
    assert_eq!(
        unversioned.execute_claim(&claim).await.state,
        "succeeded",
        "unversioned lifecycle expiry must settle"
    );
    assert_eq!(
        signed_get(&unversioned, "unversioned-current")
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    assert!(
        !signed_list_objects(&unversioned)
            .await
            .text()
            .await
            .expect("read unversioned ordinary list")
            .contains("unversioned-current")
    );
    assert!(
        !signed_list_object_versions(&unversioned, &[])
            .await
            .text()
            .await
            .expect("read unversioned version list")
            .contains("unversioned-current")
    );
    assert!(
        unversioned
            .version_rows("unversioned-current")
            .await
            .is_empty()
    );
    unversioned.assert_no_pin_removal().await;
    unversioned.shutdown().await;
}

#[tokio::test]
async fn lifecycle_expiration_current_enabled() {
    let mut enabled = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleCurrentEnabled",
        1,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&enabled, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );
    let enabled_put = signed_put(
        &enabled,
        "enabled-current",
        &[],
        b"enabled".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(enabled_put.status(), StatusCode::OK);
    let enabled_version = enabled_put.headers()["x-amz-version-id"]
        .to_str()
        .expect("enabled public version")
        .to_owned();
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &enabled,
            lifecycle_current_expiration_xml(
                "enabled-expire",
                "<Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration>",
            ),
        )
        .await
        .status(),
        StatusCode::OK
    );
    enabled.run_one_scan_page().await;
    let claim = enabled.claim_one_action().await;
    assert_eq!(enabled.execute_claim(&claim).await.state, "succeeded");
    let enabled_get = signed_get(&enabled, "enabled-current").await;
    assert_eq!(enabled_get.status(), StatusCode::NOT_FOUND);
    assert_eq!(enabled_get.headers()["x-amz-delete-marker"], "true");
    assert!(
        !signed_list_objects(&enabled)
            .await
            .text()
            .await
            .expect("read enabled ordinary list")
            .contains("enabled-current")
    );
    let enabled_versions = signed_list_object_versions(&enabled, &[])
        .await
        .text()
        .await
        .expect("read enabled version list");
    assert!(enabled_versions.contains("<DeleteMarker>"));
    assert!(enabled_versions.contains(&enabled_version));
    assert_eq!(enabled.version_rows("enabled-current").await.len(), 2);
    enabled.assert_no_pin_removal().await;
    enabled.shutdown().await;
}

#[tokio::test]
async fn lifecycle_expiration_current_suspended() {
    let mut suspended = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleCurrentSuspended",
        1,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&suspended, "Suspended")
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_put(
            &suspended,
            "suspended-current",
            &[],
            b"suspended".to_vec(),
            HeaderMap::new(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &suspended,
            lifecycle_current_expiration_xml(
                "suspended-expire",
                "<Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration>",
            ),
        )
        .await
        .status(),
        StatusCode::OK
    );
    suspended.run_one_scan_page().await;
    let claim = suspended.claim_one_action().await;
    assert_eq!(suspended.execute_claim(&claim).await.state, "succeeded");
    let suspended_get = signed_get(&suspended, "suspended-current").await;
    assert_eq!(suspended_get.status(), StatusCode::NOT_FOUND);
    assert_eq!(suspended_get.headers()["x-amz-delete-marker"], "true");
    assert_eq!(suspended_get.headers()["x-amz-version-id"], "null");
    let suspended_rows = suspended.version_rows("suspended-current").await;
    assert_eq!(suspended_rows.len(), 1);
    assert_eq!(suspended_rows[0].kind, "delete_marker");
    suspended.assert_no_pin_removal().await;
    suspended.shutdown().await;
}

#[tokio::test]
async fn lifecycle_expiration_current_marker_with_history() {
    let marker_history = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleMarkerHistory",
        1,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&marker_history, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_put(
            &marker_history,
            "marker-history",
            &[],
            b"history".to_vec(),
            HeaderMap::new(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_delete_object(&marker_history, "marker-history")
            .await
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &marker_history,
            lifecycle_current_expiration_xml(
                "marker-with-history",
                "<Expiration><Days>1</Days></Expiration>",
            ),
        )
        .await
        .status(),
        StatusCode::OK
    );
    marker_history.run_one_scan_page().await;
    assert!(marker_history.action_rows().await.is_empty());
    assert_eq!(marker_history.version_rows("marker-history").await.len(), 2);
    marker_history.assert_no_pin_removal().await;
    marker_history.shutdown().await;
}

#[tokio::test]
async fn lifecycle_expiration_current_sole_markers() {
    for (rule_id, expiration) in [
        (
            "timed-sole-marker",
            "<Expiration><Days>1</Days></Expiration>",
        ),
        (
            "immediate-sole-marker",
            "<Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>",
        ),
    ] {
        let mut sole_marker = start_lifecycle_harness(KuboScript {
            add_replies: Vec::new(),
            cat_bodies: HashMap::new(),
        })
        .await;
        assert_eq!(
            signed_put_bucket_versioning(&sole_marker, "Enabled")
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            signed_delete_object(&sole_marker, "sole-marker")
                .await
                .status(),
            StatusCode::NO_CONTENT
        );
        let marker = sole_marker.version_rows("sole-marker").await.remove(0);
        if rule_id == "timed-sole-marker" {
            let now =
                ipfs_s3_gateway::store::database_clock::database_now(sole_marker.state.store.db())
                    .await
                    .expect("read timed marker database clock");
            sole_marker
                .set_database_times(
                    &marker.id,
                    now - ChronoDuration::days(3),
                    marker.became_noncurrent_at,
                )
                .await;
        }
        assert_eq!(
            signed_put_bucket_lifecycle_configuration_xml(
                &sole_marker,
                lifecycle_current_expiration_xml(rule_id, expiration),
            )
            .await
            .status(),
            StatusCode::OK
        );
        sole_marker.run_one_scan_page().await;
        let claim = sole_marker.claim_one_action().await;
        assert_eq!(sole_marker.execute_claim(&claim).await.state, "succeeded");
        assert!(sole_marker.version_rows("sole-marker").await.is_empty());
        assert!(
            !signed_list_object_versions(&sole_marker, &[])
                .await
                .text()
                .await
                .expect("read sole-marker version list")
                .contains("<DeleteMarker>")
        );
        sole_marker.assert_no_pin_removal().await;
        sole_marker.shutdown().await;
    }
}

#[tokio::test]
async fn lifecycle_expiration_noncurrent_validation() {
    let validation = start_lifecycle_harness(KuboScript {
        add_replies: Vec::new(),
        cat_bodies: HashMap::new(),
    })
    .await;
    for invalid_xml in [
        "<LifecycleConfiguration><Rule><ID>missing-days</ID><Status>Enabled</Status><Filter/>\
         <NoncurrentVersionExpiration/></Rule></LifecycleConfiguration>"
            .to_owned(),
        "<LifecycleConfiguration><Rule><ID>newer-legacy</ID><Status>Enabled</Status>\
         <Prefix>logs/</Prefix><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays>\
         <NewerNoncurrentVersions>1</NewerNoncurrentVersions></NoncurrentVersionExpiration>\
         </Rule></LifecycleConfiguration>"
            .to_owned(),
    ] {
        let rejected =
            signed_put_bucket_lifecycle_configuration_xml(&validation, invalid_xml).await;
        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
        assert!(
            rejected
                .text()
                .await
                .expect("read noncurrent validation error")
                .contains("InvalidRequest"),
            "invalid noncurrent lifecycle configuration must be rejected"
        );
    }
    validation.shutdown().await;
}

#[tokio::test]
async fn lifecycle_expiration_noncurrent_marker() {
    let mut marker = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleNoncurrentMarkerContent",
        2,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&marker, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );
    let first = signed_put(
        &marker,
        "noncurrent-marker",
        &[],
        b"first".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(first.status(), StatusCode::OK);
    let first_version = first.headers()["x-amz-version-id"]
        .to_str()
        .expect("first noncurrent version")
        .to_owned();
    let marker_response = signed_delete_object(&marker, "noncurrent-marker").await;
    assert_eq!(marker_response.status(), StatusCode::NO_CONTENT);
    let marker_version = marker_response.headers()["x-amz-version-id"]
        .to_str()
        .expect("noncurrent marker version")
        .to_owned();
    let current = signed_put(
        &marker,
        "noncurrent-marker",
        &[],
        b"current".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(current.status(), StatusCode::OK);
    let current_version = current.headers()["x-amz-version-id"]
        .to_str()
        .expect("current noncurrent version")
        .to_owned();
    let marker_rows = marker.version_rows("noncurrent-marker").await;
    assert_eq!(
        marker_rows
            .iter()
            .map(|row| row.kind.as_str())
            .collect::<Vec<_>>(),
        vec!["object", "delete_marker", "object"]
    );
    let marker_row = marker_rows
        .iter()
        .find(|row| row.kind == "delete_marker")
        .expect("noncurrent marker row")
        .clone();
    let now = ipfs_s3_gateway::store::database_clock::database_now(marker.state.store.db())
        .await
        .expect("read noncurrent marker database clock");
    marker
        .set_database_times(
            &marker_row.id,
            marker_row.lifecycle_age_started_at,
            Some(now - ChronoDuration::days(3)),
        )
        .await;
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &marker,
            lifecycle_noncurrent_expiration_xml("expire-marker", None),
        )
        .await
        .status(),
        StatusCode::OK
    );
    marker.run_one_scan_page().await;
    let claim = marker.claim_one_action().await;
    assert_eq!(claim.action.action_kind, "expire_noncurrent");
    assert_eq!(marker.execute_claim(&claim).await.state, "succeeded");
    let marker_versions = signed_list_object_versions(&marker, &[])
        .await
        .text()
        .await
        .expect("read noncurrent marker version list");
    assert!(marker_versions.contains(&first_version));
    assert!(marker_versions.contains(&current_version));
    assert!(!marker_versions.contains(&marker_version));
    assert!(
        marker
            .version_rows("noncurrent-marker")
            .await
            .iter()
            .all(|row| row.kind == "object"),
        "exact noncurrent marker deletion must not create another marker"
    );
    marker.assert_no_pin_removal().await;
    marker.shutdown().await;
}

#[tokio::test]
async fn lifecycle_expiration_noncurrent_age_and_newer_version_thresholds() {
    let mut threshold = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleNoncurrentThreshold",
        6,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&threshold, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );
    let boundary_old = signed_put(
        &threshold,
        "strict-age",
        &[],
        b"old".to_vec(),
        HeaderMap::new(),
    )
    .await;
    let boundary_old_version = boundary_old.headers()["x-amz-version-id"]
        .to_str()
        .expect("strict age old version")
        .to_owned();
    assert_eq!(boundary_old.status(), StatusCode::OK);
    assert_eq!(
        signed_put(
            &threshold,
            "strict-age",
            &[],
            b"new".to_vec(),
            HeaderMap::new(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &threshold,
            lifecycle_noncurrent_expiration_xml("strict-age", None),
        )
        .await
        .status(),
        StatusCode::OK
    );
    let boundary_row = threshold.version_rows("strict-age").await.remove(0);
    let now = ipfs_s3_gateway::store::database_clock::database_now(threshold.state.store.db())
        .await
        .expect("read strict-age database clock");
    threshold
        .set_database_times(
            &boundary_row.id,
            boundary_row.lifecycle_age_started_at,
            Some(now - ChronoDuration::days(1)),
        )
        .await;
    threshold.run_one_scan_page().await;
    assert!(
        threshold.action_rows().await.is_empty(),
        "one incomplete noncurrent day must remain before its strict UTC boundary"
    );
    threshold
        .set_database_times(
            &boundary_row.id,
            boundary_row.lifecycle_age_started_at,
            Some(now - ChronoDuration::days(3)),
        )
        .await;
    threshold.run_one_scan_page().await;
    let claim = threshold.claim_one_action().await;
    assert_eq!(threshold.execute_claim(&claim).await.state, "succeeded");
    assert_eq!(
        signed_get_version(&threshold, "strict-age", &boundary_old_version)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );

    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &threshold,
            lifecycle_noncurrent_expiration_xml("newer-threshold", Some(1)),
        )
        .await
        .status(),
        StatusCode::OK
    );
    let oldest = signed_put(
        &threshold,
        "newer-threshold",
        &[],
        b"one".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(oldest.status(), StatusCode::OK);
    let oldest_version = oldest.headers()["x-amz-version-id"]
        .to_str()
        .expect("oldest threshold version")
        .to_owned();
    let middle = signed_put(
        &threshold,
        "newer-threshold",
        &[],
        b"two".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(middle.status(), StatusCode::OK);
    let middle_version = middle.headers()["x-amz-version-id"]
        .to_str()
        .expect("middle threshold version")
        .to_owned();
    assert_eq!(
        signed_put(
            &threshold,
            "newer-threshold",
            &[],
            b"three".to_vec(),
            HeaderMap::new(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    let oldest_row = threshold.version_rows("newer-threshold").await.remove(0);
    threshold
        .set_database_times(
            &oldest_row.id,
            oldest_row.lifecycle_age_started_at,
            Some(now - ChronoDuration::days(3)),
        )
        .await;
    threshold.run_one_scan_page().await;
    assert!(
        threshold
            .action_rows()
            .await
            .iter()
            .all(|action| action.state != "pending"),
        "exactly N newer noncurrent versions must not expire the target"
    );
    let successor = signed_put(
        &threshold,
        "newer-threshold",
        &[],
        b"four".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(successor.status(), StatusCode::OK);
    let successor_version = successor.headers()["x-amz-version-id"]
        .to_str()
        .expect("successor threshold version")
        .to_owned();
    threshold.run_one_scan_page().await;
    let claim = threshold.claim_one_action().await;
    assert_eq!(
        claim.action.target_public_version_id.as_deref(),
        Some(oldest_version.as_str())
    );
    assert_eq!(threshold.execute_claim(&claim).await.state, "succeeded");
    let rows_after = threshold.version_rows("newer-threshold").await;
    assert!(
        rows_after
            .iter()
            .all(|row| row.version_id.as_deref() != Some(oldest_version.as_str()))
    );
    assert!(
        rows_after.iter().any(
            |row| row.version_id.as_deref() == Some(successor_version.as_str()) && row.is_latest
        ),
        "exact noncurrent deletion must not mutate the successor"
    );
    let threshold_versions = signed_list_object_versions(&threshold, &[])
        .await
        .text()
        .await
        .expect("read newer-threshold version list");
    assert!(threshold_versions.contains(&middle_version));
    assert!(threshold_versions.contains(&successor_version));
    assert!(!threshold_versions.contains(&oldest_version));
    threshold.assert_no_pin_removal().await;
    threshold.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_expiration_invariants_deleted_configuration() {
    let mut deleted_config = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleDeletedConfig",
        1,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put(
            &deleted_config,
            "delete-config",
            &[],
            b"body".to_vec(),
            HeaderMap::new(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    let claim = schedule_lifecycle_action(
        &deleted_config,
        lifecycle_current_expiration_xml(
            "delete-config",
            "<Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration>",
        ),
    )
    .await;
    assert_eq!(
        signed_delete_bucket_lifecycle_configuration(&deleted_config)
            .await
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        deleted_config.execute_claim(&claim).await.state,
        "cancelled"
    );
    assert_eq!(deleted_config.version_rows("delete-config").await.len(), 1);
    deleted_config.assert_no_pin_removal().await;
    deleted_config.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_expiration_invariants_disabled_rule() {
    let mut disabled_rule = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleDisabledRule",
        1,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put(
            &disabled_rule,
            "disabled-rule",
            &[],
            b"body".to_vec(),
            HeaderMap::new(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    let claim = schedule_lifecycle_action(
        &disabled_rule,
        lifecycle_current_expiration_xml(
            "disable-after-claim",
            "<Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration>",
        ),
    )
    .await;
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &disabled_rule,
            "<LifecycleConfiguration><Rule><ID>disable-after-claim</ID><Status>Disabled</Status>\
             <Filter/><Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration>\
             </Rule></LifecycleConfiguration>"
                .to_owned(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(disabled_rule.execute_claim(&claim).await.state, "cancelled");
    assert_eq!(disabled_rule.version_rows("disabled-rule").await.len(), 1);
    disabled_rule.assert_no_pin_removal().await;
    disabled_rule.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_expiration_invariants_changed_tags() {
    let mut changed_tags = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleChangedTags",
        1,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put_with_tagging(
            &changed_tags,
            "changed-tags",
            b"body".to_vec(),
            "class=keep"
        )
        .await
        .status(),
        StatusCode::OK
    );
    let claim = schedule_lifecycle_action(
        &changed_tags,
        "<LifecycleConfiguration><Rule><ID>recheck-tags</ID><Status>Enabled</Status>\
         <Filter><Tag><Key>class</Key><Value>keep</Value></Tag></Filter>\
         <Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration></Rule>\
         </LifecycleConfiguration>"
            .to_owned(),
    )
    .await;
    assert_eq!(
        signed_put_object_tagging(&changed_tags, "changed-tags", &[("class", "changed")])
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(changed_tags.execute_claim(&claim).await.state, "cancelled");
    assert_eq!(changed_tags.version_rows("changed-tags").await.len(), 1);
    changed_tags.assert_no_pin_removal().await;
    changed_tags.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_expiration_invariants_replaced_current() {
    let mut replaced_current = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleReplaceCurrent",
        2,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put_with_tagging(
            &replaced_current,
            "replace-current",
            b"old".to_vec(),
            "generation=old",
        )
        .await
        .status(),
        StatusCode::OK
    );
    let claim = schedule_lifecycle_action(
        &replaced_current,
        "<LifecycleConfiguration><Rule><ID>replace-current</ID><Status>Enabled</Status>\
         <Filter><Tag><Key>generation</Key><Value>old</Value></Tag></Filter>\
         <Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration></Rule>\
         </LifecycleConfiguration>"
            .to_owned(),
    )
    .await;
    assert_eq!(
        signed_put_with_tagging(
            &replaced_current,
            "replace-current",
            b"replacement".to_vec(),
            "generation=new",
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        replaced_current.execute_claim(&claim).await.state,
        "cancelled"
    );
    assert_eq!(
        replaced_current.version_rows("replace-current").await.len(),
        1
    );
    replaced_current.assert_no_pin_removal().await;
    replaced_current.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_expiration_invariants_noncurrent_target_changes() {
    for (key, mutation) in [
        ("promoted-target", "promote"),
        ("exact-target", "exact-delete"),
    ] {
        let mut noncurrent = start_lifecycle_harness(KuboScript::repeated_add(
            "QmLifecycleNoncurrentInvariant",
            2,
            HashMap::new(),
        ))
        .await;
        assert_eq!(
            signed_put_bucket_versioning(&noncurrent, "Enabled")
                .await
                .status(),
            StatusCode::OK
        );
        let old = signed_put(&noncurrent, key, &[], b"old".to_vec(), HeaderMap::new()).await;
        assert_eq!(old.status(), StatusCode::OK);
        let old_version = old.headers()["x-amz-version-id"]
            .to_str()
            .expect("old invariant version")
            .to_owned();
        let current =
            signed_put(&noncurrent, key, &[], b"current".to_vec(), HeaderMap::new()).await;
        assert_eq!(current.status(), StatusCode::OK);
        let current_version = current.headers()["x-amz-version-id"]
            .to_str()
            .expect("current invariant version")
            .to_owned();
        let old_row = noncurrent.version_rows(key).await.remove(0);
        let now = ipfs_s3_gateway::store::database_clock::database_now(noncurrent.state.store.db())
            .await
            .expect("read invariant noncurrent database clock");
        noncurrent
            .set_database_times(
                &old_row.id,
                old_row.lifecycle_age_started_at,
                Some(now - ChronoDuration::days(3)),
            )
            .await;
        let claim = schedule_lifecycle_action(
            &noncurrent,
            lifecycle_noncurrent_expiration_xml("noncurrent-invariant", None),
        )
        .await;
        assert_eq!(
            claim.action.target_public_version_id.as_deref(),
            Some(old_version.as_str())
        );
        if mutation == "promote" {
            assert_eq!(
                signed_delete_object_version(&noncurrent, key, Some(&current_version))
                    .await
                    .status(),
                StatusCode::NO_CONTENT
            );
            assert!(
                noncurrent
                    .version_rows(key)
                    .await
                    .iter()
                    .any(
                        |row| row.version_id.as_deref() == Some(old_version.as_str())
                            && row.is_latest
                    )
            );
        } else {
            assert_eq!(
                signed_delete_object_version(&noncurrent, key, Some(&old_version))
                    .await
                    .status(),
                StatusCode::NO_CONTENT
            );
            assert!(
                noncurrent
                    .version_rows(key)
                    .await
                    .iter()
                    .any(
                        |row| row.version_id.as_deref() == Some(current_version.as_str())
                            && row.is_latest
                    )
            );
        }
        assert_eq!(noncurrent.execute_claim(&claim).await.state, "cancelled");
        noncurrent.assert_no_pin_removal().await;
        noncurrent.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_expiration_invariants_publication_delete_race() {
    let mut publication_delete_race = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecyclePublicationDeleteRace",
        3,
        HashMap::from([(
            "QmLifecyclePublicationDeleteRace".to_owned(),
            b"replacement".to_vec(),
        )]),
    ))
    .await;
    assert_eq!(
        signed_put_with_tagging(
            &publication_delete_race,
            "publication-delete-race",
            b"old".to_vec(),
            "generation=old",
        )
        .await
        .status(),
        StatusCode::OK
    );
    let claim = schedule_lifecycle_action(
        &publication_delete_race,
        "<LifecycleConfiguration><Rule><ID>publication-delete-race</ID><Status>Enabled</Status>\
         <Filter><Tag><Key>generation</Key><Value>old</Value></Tag></Filter>\
         <Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration></Rule>\
         </LifecycleConfiguration>"
            .to_owned(),
    )
    .await;
    let endpoint = OwnedTestEndpoint {
        endpoint: publication_delete_race.endpoint.clone(),
        bucket: publication_delete_race.bucket.clone(),
    };
    // Hold the admitted PUT before publication so DELETE supersedes its fence.
    let mut block = support::decompress::block_next_kubo_request(
        &publication_delete_race.kubo,
        KuboBlockTarget::PinAdd,
        wiremock::ResponseTemplate::new(200).set_body_string("{\"Pins\":[]}"),
    )
    .await;
    let task = tokio::spawn(async move {
        signed_put_with_tagging(
            &endpoint,
            "publication-delete-race",
            b"replacement".to_vec(),
            "generation=new",
        )
        .await
    });
    block.wait_until_blocked().await;
    let deleted = signed_delete_object(&publication_delete_race, "publication-delete-race").await;
    block.release();
    let published = tokio::time::timeout(std::time::Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    assert_eq!(published.status(), StatusCode::CONFLICT);
    assert_mpu_s3_error(published, "OperationAborted").await;
    assert!(
        publication_delete_race
            .version_rows("publication-delete-race")
            .await
            .is_empty(),
        "the superseded PUT must not resurrect the deleted object"
    );

    assert_eq!(
        signed_put_with_tagging(
            &publication_delete_race,
            "publication-delete-race",
            b"replacement".to_vec(),
            "generation=new",
        )
        .await
        .status(),
        StatusCode::OK,
        "a serialized retry of the aborted PUT must publish"
    );
    let replacement = publication_delete_race
        .version_rows("publication-delete-race")
        .await;
    assert_eq!(replacement.len(), 1);
    assert_eq!(
        publication_delete_race.execute_claim(&claim).await.state,
        "cancelled"
    );
    assert_eq!(
        publication_delete_race
            .version_rows("publication-delete-race")
            .await,
        replacement,
        "the old lifecycle claim must leave the replacement untouched"
    );
    assert_signed_body(
        &publication_delete_race,
        "publication-delete-race",
        b"replacement",
    )
    .await;
    assert_eq!(
        signed_delete_object(&publication_delete_race, "publication-delete-race")
            .await
            .status(),
        StatusCode::NO_CONTENT
    );
    assert!(
        publication_delete_race
            .version_rows("publication-delete-race")
            .await
            .is_empty()
    );
    assert_mpu_s3_error(
        signed_get(&publication_delete_race, "publication-delete-race").await,
        "NoSuchKey",
    )
    .await;
    publication_delete_race.assert_no_pin_removal().await;
    publication_delete_race.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_expiration_invariants_shared_cid() {
    let mut shared_cid = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleSharedCid",
        2,
        HashMap::from([("QmLifecycleSharedCid".to_owned(), b"shared".to_vec())]),
    ))
    .await;
    for key in ["shared-a", "shared-b"] {
        assert_eq!(
            signed_put(&shared_cid, key, &[], b"shared".to_vec(), HeaderMap::new())
                .await
                .status(),
            StatusCode::OK
        );
    }
    let claim = schedule_lifecycle_action(
        &shared_cid,
        "<LifecycleConfiguration><Rule><ID>shared-cid</ID><Status>Enabled</Status>\
         <Filter><Prefix>shared-a</Prefix></Filter>\
         <Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration></Rule>\
         </LifecycleConfiguration>"
            .to_owned(),
    )
    .await;
    assert_eq!(shared_cid.execute_claim(&claim).await.state, "succeeded");
    assert_eq!(shared_cid.version_rows("shared-a").await.len(), 0);
    assert_signed_body(&shared_cid, "shared-b", b"shared").await;
    shared_cid.assert_no_pin_removal().await;
    shared_cid.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_expiration_invariants_encryption_matrix() {
    for (key, headers, expect_sse_s3) in [
        ("plain", HeaderMap::new(), false),
        (
            "sse-s3",
            HeaderMap::from_iter([(
                http::HeaderName::from_static("x-amz-server-side-encryption"),
                HeaderValue::from_static("AES256"),
            )]),
            true,
        ),
        ("sse-c", sse_c_headers(), false),
    ] {
        let mut encrypted = start_lifecycle_harness(KuboScript::repeated_add(
            "QmLifecycleEncrypted",
            1,
            HashMap::new(),
        ))
        .await;
        assert_eq!(
            signed_put(&encrypted, key, &[], b"protected".to_vec(), headers)
                .await
                .status(),
            StatusCode::OK
        );
        let before = store::object::get_latest(encrypted.state.store.db(), &encrypted.bucket, key)
            .await
            .expect("load lifecycle encryption target");
        assert_eq!(before.encrypted, key != "plain");
        assert_eq!(before.key_wrap.is_some(), expect_sse_s3);
        assert_eq!(before.sse_c_key_fingerprint.is_some(), key == "sse-c");
        let claim = schedule_lifecycle_action(
            &encrypted,
            lifecycle_current_expiration_xml(
                "encrypted-expire",
                "<Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration>",
            ),
        )
        .await;
        assert_eq!(encrypted.execute_claim(&claim).await.state, "succeeded");
        assert!(encrypted.version_rows(key).await.is_empty());
        encrypted.assert_no_pin_removal().await;
        encrypted.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_expiration_invariants_tags_leases_retry() {
    let mut tags_leases_retry = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleLeaseRetry",
        1,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put_with_tagging(
            &tags_leases_retry,
            "tags-leases",
            b"body".to_vec(),
            "class=old"
        )
        .await
        .status(),
        StatusCode::OK
    );
    let object = store::object::get_latest(
        tags_leases_retry.state.store.db(),
        &tags_leases_retry.bucket,
        "tags-leases",
    )
    .await
    .expect("load tags and leases lifecycle object");
    let now =
        ipfs_s3_gateway::store::database_clock::database_now(tags_leases_retry.state.store.db())
            .await
            .expect("read tags and leases database clock");
    let lease_id = uuid::Uuid::new_v4().to_string();
    store::entities::pin_lease::Entity::insert(store::entities::pin_lease::ActiveModel {
        id: Set(lease_id.clone()),
        owner_object_id: Set(object.id.clone()),
        source: Set("lifecycle-acceptance".to_owned()),
        policy_id: Set("lifecycle-acceptance".to_owned()),
        provider_mode: Set("one".to_owned()),
        content_mode: Set("object".to_owned()),
        created_at: Set(now),
        last_touched_at: Set(now),
        expires_at: Set(now + ChronoDuration::days(1)),
        generation: Set(1),
        state: Set("active".to_owned()),
    })
    .exec(tags_leases_retry.state.store.db())
    .await
    .expect("seed active lifecycle lease");
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &tags_leases_retry,
            lifecycle_current_expiration_xml(
                "tags-leases-retry",
                "<Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration>",
            ),
        )
        .await
        .status(),
        StatusCode::OK
    );
    tags_leases_retry.run_one_scan_page().await;
    let claim = tags_leases_retry.stop_after_claim().await;
    let terminal = tags_leases_retry.execute_claim(&claim).await;
    assert_eq!(terminal.state, "succeeded");
    assert_eq!(
        terminal.attempts, 2,
        "stopped claimed work must be reclaimed once"
    );
    assert_eq!(
        store::entities::object_tag::Entity::find()
            .filter(store::entities::object_tag::Column::ObjectId.eq(&object.id))
            .count(tags_leases_retry.state.store.db())
            .await
            .expect("count cleared lifecycle tags"),
        0
    );
    assert_eq!(
        store::entities::pin_lease::Entity::find_by_id(lease_id)
            .one(tags_leases_retry.state.store.db())
            .await
            .expect("load ended lifecycle lease")
            .expect("seeded lifecycle lease exists")
            .state,
        "cancelled"
    );
    tags_leases_retry.assert_no_pin_removal().await;
    tags_leases_retry.shutdown().await;
}
