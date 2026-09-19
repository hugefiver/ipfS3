use std::collections::HashMap;

use http::{HeaderMap, HeaderValue, StatusCode, header};
use sea_orm::EntityTrait;

use ipfs_s3_gateway::{
    lifecycle::config::{canonical_json, validate_and_canonicalize},
    store::{entities::bucket_lifecycle_config, lifecycle_config::put_configuration},
};

use super::{LifecycleHarness, start_lifecycle_harness, start_lifecycle_harness_with_cold};
use crate::support::{decompress::KuboScript, sigv4::send_sigv4};

fn empty_script() -> KuboScript {
    KuboScript {
        add_replies: Vec::new(),
        cat_bodies: HashMap::new(),
    }
}

async fn signed_put(harness: &LifecycleHarness, xml: &str) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[("lifecycle", "")],
        xml.as_bytes().to_vec(),
        headers,
        "test",
    )
    .await
}

async fn signed_get(harness: &LifecycleHarness) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[("lifecycle", "")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

async fn signed_delete(harness: &LifecycleHarness) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::DELETE,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[("lifecycle", "")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

fn lifecycle_xml(rule: &str, action: &str) -> String {
    format!(
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ID>{rule}</ID><Status>Enabled</Status><Filter/>{action}</Rule>\
         </LifecycleConfiguration>"
    )
}

async fn stored_configuration(harness: &LifecycleHarness) -> bucket_lifecycle_config::Model {
    bucket_lifecycle_config::Entity::find_by_id(&harness.bucket)
        .one(harness.state.store.db())
        .await
        .expect("load lifecycle configuration")
        .expect("lifecycle configuration row exists")
}

async fn assert_invalid_request(response: reqwest::Response) {
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response.text().await.expect("read lifecycle error XML");
    assert!(
        body.contains("<Code>InvalidRequest</Code>"),
        "expected InvalidRequest response: {body}"
    );
}

#[tokio::test]
async fn signed_transition_put_get_accepts_supported_forms_only_with_cold_kubo() {
    let harness = start_lifecycle_harness_with_cold(empty_script()).await;

    for (rule, action, expected_fragments) in [
        (
            "current-days",
            "<Transition><Days>7</Days><StorageClass>STANDARD_IA</StorageClass></Transition>",
            vec![
                "<Transition>",
                "<Days>7</Days>",
                "<StorageClass>STANDARD_IA</StorageClass>",
            ],
        ),
        (
            "current-date",
            "<Transition><Date>2030-01-01T00:00:00Z</Date><StorageClass>STANDARD_IA</StorageClass></Transition>",
            vec![
                "<Transition>",
                "<Date>2030-01-01T00:00:00.000Z</Date>",
                "<StorageClass>STANDARD_IA</StorageClass>",
            ],
        ),
        (
            "noncurrent",
            "<NoncurrentVersionTransition><NoncurrentDays>9</NoncurrentDays><StorageClass>STANDARD_IA</StorageClass></NoncurrentVersionTransition>",
            vec![
                "<NoncurrentVersionTransition>",
                "<NoncurrentDays>9</NoncurrentDays>",
                "<StorageClass>STANDARD_IA</StorageClass>",
            ],
        ),
        (
            "current-and-noncurrent",
            "<Transition><Days>7</Days><StorageClass>STANDARD_IA</StorageClass></Transition>\
             <NoncurrentVersionTransition><NoncurrentDays>9</NoncurrentDays><NewerNoncurrentVersions>2</NewerNoncurrentVersions><StorageClass>STANDARD_IA</StorageClass></NoncurrentVersionTransition>",
            vec![
                "<Transition>",
                "<Days>7</Days>",
                "<NoncurrentDays>9</NoncurrentDays>",
                "<NewerNoncurrentVersions>2</NewerNoncurrentVersions>",
            ],
        ),
    ] {
        let put = signed_put(&harness, &lifecycle_xml(rule, action)).await;
        assert_eq!(put.status(), StatusCode::OK, "PUT {rule}");

        let get = signed_get(&harness).await;
        assert_eq!(get.status(), StatusCode::OK, "GET {rule}");
        let body = get.text().await.expect("read lifecycle transition XML");
        assert!(body.contains(&format!("<ID>{rule}</ID>")), "{body}");
        for fragment in expected_fragments {
            assert!(body.contains(fragment), "missing {fragment}: {body}");
        }
    }

    let before_invalid = stored_configuration(&harness).await;
    for action in [
        "<Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition>",
        "<Transition><Days>1</Days><Date>2030-01-01T00:00:00Z</Date><StorageClass>STANDARD_IA</StorageClass></Transition>",
        "<NoncurrentVersionTransition><StorageClass>STANDARD_IA</StorageClass></NoncurrentVersionTransition>",
        "<NoncurrentVersionTransition><NoncurrentDays>9</NoncurrentDays><StorageClass>GLACIER</StorageClass></NoncurrentVersionTransition>",
        "<Transition><Date>2030-01-01T01:00:00Z</Date><StorageClass>STANDARD_IA</StorageClass></Transition>",
    ] {
        assert_invalid_request(signed_put(&harness, &lifecycle_xml("invalid", action)).await).await;
        assert_eq!(stored_configuration(&harness).await, before_invalid);
    }

    assert!(
        harness.kubo.received_requests().await.unwrap().is_empty(),
        "lifecycle API gate must not call hot Kubo"
    );
    assert!(
        harness
            .cold_kubo
            .as_ref()
            .expect("cold Kubo is configured")
            .received_requests()
            .await
            .unwrap()
            .is_empty(),
        "lifecycle API gate must not call or health-check cold Kubo"
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn signed_no_cold_transition_rejection_is_atomic_and_get_delete_still_work() {
    let harness = start_lifecycle_harness(empty_script()).await;
    let original = lifecycle_xml("existing", "<Expiration><Days>30</Days></Expiration>");
    assert_eq!(
        signed_put(&harness, &original).await.status(),
        StatusCode::OK
    );
    let before = stored_configuration(&harness).await;

    for action in [
        "<Transition><Days>7</Days><StorageClass>STANDARD_IA</StorageClass></Transition>",
        "<NoncurrentVersionTransition><NoncurrentDays>9</NoncurrentDays><StorageClass>STANDARD_IA</StorageClass></NoncurrentVersionTransition>",
    ] {
        let rejected = lifecycle_xml(
            "whole-put-rejected",
            &format!("<Expiration><Days>60</Days></Expiration>{action}"),
        );
        assert_invalid_request(signed_put(&harness, &rejected).await).await;
        assert_eq!(stored_configuration(&harness).await, before);
    }

    let get = signed_get(&harness).await;
    assert_eq!(get.status(), StatusCode::OK);
    let body = get.text().await.expect("read preserved lifecycle XML");
    assert!(body.contains("<ID>existing</ID>"), "{body}");
    assert!(body.contains("<Days>30</Days>"), "{body}");
    assert!(!body.contains("whole-put-rejected"), "{body}");

    let existing_transition = serde_json::from_value(serde_json::json!({
        "rules": [{
            "id": "persisted-transition",
            "filter": {},
            "status": "Enabled",
            "transitions": [{ "days": 11, "storage_class": "STANDARD_IA" }]
        }]
    }))
    .expect("build pre-existing transition configuration");
    let canonical = validate_and_canonicalize(existing_transition)
        .expect("canonicalize pre-existing transition configuration");
    put_configuration(
        harness.state.store.db(),
        &harness.bucket,
        &canonical_json(&canonical).expect("serialize pre-existing transition configuration"),
    )
    .await
    .expect("seed transition configuration created while cold Kubo was available");

    let get = signed_get(&harness).await;
    assert_eq!(get.status(), StatusCode::OK);
    let body = get
        .text()
        .await
        .expect("read pre-existing transition configuration");
    assert!(body.contains("<ID>persisted-transition</ID>"), "{body}");
    assert!(body.contains("<Transition>"), "{body}");
    assert!(body.contains("<Days>11</Days>"), "{body}");

    assert_eq!(
        signed_delete(&harness).await.status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(signed_get(&harness).await.status(), StatusCode::NOT_FOUND);
    assert!(
        harness.kubo.received_requests().await.unwrap().is_empty(),
        "lifecycle GET and DELETE must not call or health-check Kubo"
    );
    harness.shutdown().await;
}
