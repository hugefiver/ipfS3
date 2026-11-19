use std::time::Duration;

use chrono::{TimeDelta, Utc};
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use serde_json::Value;
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{body_partial_json, method, path},
};

use crate::{
    assert_get,
    fixture::{ARCHIVE_KEY, BUCKET, Fixture, OUTPUT_BODY, OUTPUT_CID},
    pin_rows,
    support::{decompress::S3TestEndpoint, sigv4::send_sigv4},
};

async fn request(
    fixture: &Fixture,
    method: Method,
    key: &str,
    query: &[(&str, &str)],
    xml: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    if !xml.is_empty() {
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/xml"),
        );
    }
    send_sigv4(
        method,
        fixture.endpoint(),
        BUCKET,
        key,
        query,
        xml.as_bytes().to_vec(),
        headers,
        "test",
    )
    .await
}

async fn pin_status(fixture: &Fixture, key: &str, version: &str) -> Vec<Value> {
    let response = request(
        fixture,
        Method::GET,
        key,
        &[("ipfs3-pin-status", ""), ("versionId", version)],
        "",
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "pin status for {key} at {version}"
    );
    assert_eq!(response.headers()["x-amz-version-id"], version);
    let body: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(body["version_id"], version);
    body["references"].as_array().unwrap().clone()
}

fn renew_xml(until: chrono::DateTime<Utc>) -> String {
    format!(
        "<Tagging><TagSet><Tag><Key>ipfs-s3:pin</Key><Value>true</Value></Tag><Tag><Key>ipfs-s3:retain-until</Key><Value>{}</Value></Tag></TagSet></Tagging>",
        until.to_rfc3339()
    )
}

async fn wait_for_resource(fixture: &Fixture, status: &str) {
    let db = fixture.state.store.db();
    let result = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let resources = pin_rows::resources(db).await;
            if matches!(&resources[..], [resource] if resource.status == status && resource.action.is_none()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await;
    assert!(
        result.is_ok(),
        "shared resource did not become {status}: {:?}",
        pin_rows::resources(db).await
    );
}

pub async fn assert_file_lifecycle(fixture: &mut Fixture, outputs: &[(String, String)]) {
    let head = request(fixture, Method::HEAD, ARCHIVE_KEY, &[], "").await;
    assert_eq!(head.status(), StatusCode::OK);
    let archive_version = head.headers()["x-amz-version-id"].to_str().unwrap();
    let archive_query = [("versionId", archive_version)];
    let archive_tags = request(
        fixture,
        Method::GET,
        ARCHIVE_KEY,
        &[("tagging", ""), archive_query[0]],
        "",
    )
    .await;
    assert_eq!(archive_tags.status(), StatusCode::OK);
    let tags = archive_tags.text().await.unwrap();
    assert!(
        tags.contains("<Key>ipfs-s3:pin</Key>") && tags.contains("<Value>archive</Value>"),
        "{tags}"
    );
    assert!(
        pin_status(fixture, ARCHIVE_KEY, archive_version)
            .await
            .is_empty()
    );
    let now = Utc::now();
    let in_policy = now + TimeDelta::minutes(90);
    let renew = request(
        fixture,
        Method::PUT,
        ARCHIVE_KEY,
        &[("tagging", ""), archive_query[0]],
        &renew_xml(in_policy),
    )
    .await;
    assert_eq!(
        renew.status(),
        StatusCode::BAD_REQUEST,
        "archive has no manual reference to renew"
    );
    let deleted = request(fixture, Method::DELETE, ARCHIVE_KEY, &archive_query, "").await;
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    let deleted = request(fixture, Method::HEAD, ARCHIVE_KEY, &archive_query, "").await;
    assert_eq!(deleted.status(), StatusCode::NOT_FOUND);

    Mock::given(method("POST"))
        .and(path("/psa/pins"))
        .and(body_partial_json(serde_json::json!({"cid": OUTPUT_CID})))
        .respond_with(ResponseTemplate::new(202).set_body_json(serde_json::json!({
            "requestid": "shared-output", "status": "queued", "pin": {"cid": OUTPUT_CID},
        })))
        .mount(&fixture.kubo)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/psa/pins/shared-output"))
        .respond_with(ResponseTemplate::new(202))
        .mount(&fixture.kubo)
        .await;
    fixture.start_pin_worker();
    wait_for_resource(fixture, "accepted").await;
    for (key, version) in outputs {
        let references = pin_status(fixture, key, version).await;
        assert_eq!(references.len(), 1, "{key} {version}");
        let reference = &references[0];
        assert_eq!(reference["source"], "manual");
        assert_eq!(reference["cid"], OUTPUT_CID);
        assert_eq!(reference["provider"], fixture.provider);
        assert_eq!(reference["status"], "accepted");
        let tags = request(
            fixture,
            Method::GET,
            key,
            &[("tagging", ""), ("versionId", version)],
            "",
        )
        .await;
        assert_eq!(tags.status(), StatusCode::OK);
        assert!(
            !tags.text().await.unwrap().contains("<Tag>"),
            "publication must not copy archive tags to {key}"
        );
    }

    let [(first, first_version), (second, second_version)] = outputs else {
        panic!("expected two exact file versions");
    };
    let first_query = [("versionId", first_version.as_str())];
    let second_tagging = [("tagging", ""), ("versionId", second_version.as_str())];
    let refused = request(
        fixture,
        Method::PUT,
        second,
        &second_tagging,
        &renew_xml(now + TimeDelta::hours(3)),
    )
    .await;
    assert_eq!(
        refused.status(),
        StatusCode::BAD_REQUEST,
        "renewal must obey the file's 2h policy"
    );
    let renewed = request(
        fixture,
        Method::PUT,
        second,
        &second_tagging,
        &renew_xml(in_policy),
    )
    .await;
    assert_eq!(
        renewed.status(),
        StatusCode::OK,
        "{}",
        renewed.text().await.unwrap()
    );
    let second_references = pin_status(fixture, second, second_version).await;
    let expiry: chrono::DateTime<Utc> = second_references[0]["expires_at"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(expiry.timestamp_micros(), in_policy.timestamp_micros());
    let db = fixture.state.store.db();
    let before = pin_rows::refs(db).await;
    assert_eq!(before.len(), 2);
    assert!(
        before
            .iter()
            .all(|reference| reference.content_mode == "decompressed")
    );
    assert_eq!(
        before
            .iter()
            .filter(|reference| reference.expires_at == in_policy.timestamp_micros())
            .count(),
        1
    );

    let deleted = request(fixture, Method::DELETE, first, &first_query, "").await;
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    let missing = request(
        fixture,
        Method::GET,
        first,
        &[("ipfs3-pin-status", ""), first_query[0]],
        "",
    )
    .await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        pin_status(fixture, second, second_version).await,
        second_references
    );
    assert_eq!(pin_rows::refs(db).await.len(), 1);
    let resource = pin_rows::resources(db).await.remove(0);
    assert_eq!(resource.status, "accepted");
    assert_eq!(
        resource.action, None,
        "one remaining file reference keeps the backup"
    );
    assert!(
        fixture
            .kubo
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method != Method::DELETE)
    );

    let cancelled = request(fixture, Method::DELETE, second, &second_tagging, "").await;
    assert_eq!(cancelled.status(), StatusCode::NO_CONTENT);
    assert!(pin_status(fixture, second, second_version).await.is_empty());
    assert!(pin_rows::refs(db).await.is_empty());
    wait_for_resource(fixture, "cleaned").await;
    let calls = fixture.kubo.received_requests().await.unwrap();
    assert_eq!(
        calls
            .iter()
            .filter(|r| r.url.path().starts_with("/psa/"))
            .map(|r| (r.method.as_str(), r.url.path()))
            .collect::<Vec<_>>(),
        [("POST", "/psa/pins"), ("DELETE", "/psa/pins/shared-output")]
    );
    assert!(calls.iter().all(|r| r.url.path() != "/api/v0/pin/rm"));
    assert_get(fixture, second, Some(second_version), OUTPUT_BODY).await;
}
