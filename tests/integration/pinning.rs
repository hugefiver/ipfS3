use super::common::{
    assert_no_kubo_pin_removes, assert_put_cid_headers, assert_signed_body, signed_copy,
    signed_delete_object_tagging, signed_get_object_tagging, signed_put_object_tagging,
    signed_put_with_tagging, tagging_pairs,
};
use crate::support::{
    pin_rows,
    pinning::{PinningHarnessConfig, TEST_CID, start_pinning_harness},
};
use http::{HeaderMap, StatusCode};
use ipfs_s3_gateway::pinning::provider::canonical_cid;
use sea_orm::ConnectionTrait;
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{header, method, path},
};

#[tokio::test]
async fn signed_publication_copy_renewal_and_final_expiry_use_the_accepted_handle() {
    let mut config = PinningHarnessConfig::request_one();
    config.pinata_script.push(
        Mock::given(method("DELETE"))
            .and(path("/psa/pins/request-1"))
            .and(header("Authorization", "Bearer pinning-test-token"))
            .respond_with(ResponseTemplate::new(202))
            .expect(1),
    );
    let harness = start_pinning_harness(config).await;
    let put = signed_put_with_tagging(
        &harness,
        "source.txt",
        b"body".to_vec(),
        "team=storage&ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_cid_headers(&put, TEST_CID);
    let copy = signed_copy(&harness, "source.txt", "copy.txt", HeaderMap::new()).await;
    assert_eq!(copy.status(), StatusCode::OK);
    let tags = signed_get_object_tagging(&harness, "copy.txt").await;
    assert_eq!(tags.status(), StatusCode::OK);
    assert_eq!(
        tagging_pairs(&tags.text().await.unwrap()),
        [
            ("ipfs-s3:pin".into(), "true".into()),
            ("team".into(), "storage".into())
        ]
    );
    let db = harness.state.store.db();
    let refs = pin_rows::refs(db).await;
    assert_eq!(refs.len(), 2);
    assert_ne!(refs[0].owner_object_id, refs[1].owner_object_id);
    let cid = canonical_cid(TEST_CID).unwrap();
    assert!(refs.iter().all(|r| r.cid == cid && r.source == "manual"));
    assert!(harness.pinata.received_requests().await.unwrap().is_empty());
    harness.run_until_resource_status("accepted").await;
    let resource = pin_rows::resources(db).await.remove(0);
    assert_eq!(
        (
            resource.cid,
            resource.logical_size,
            resource.handle.as_deref()
        ),
        (cid, 4, Some("request-1"))
    );
    let retain_until = (chrono::Utc::now() + chrono::Duration::hours(2)).timestamp_micros();
    let retain_tag = chrono::DateTime::from_timestamp_micros(retain_until)
        .unwrap()
        .to_rfc3339();
    let renew = signed_put_object_tagging(
        &harness,
        "copy.txt",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", &retain_tag),
        ],
    )
    .await;
    assert_eq!(renew.status(), StatusCode::OK);
    let renewed = pin_rows::refs(db).await;
    assert_eq!(renewed.len(), 2);
    assert!(renewed.iter().any(|r| r.expires_at == retain_until));
    assert_eq!(
        signed_delete_object_tagging(&harness, "source.txt")
            .await
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(pin_rows::refs(db).await.len(), 1);
    assert_eq!(pin_rows::resources(db).await[0].action, None);
    assert_eq!(
        db.execute_unprepared("UPDATE pin_refs SET expires_at=1")
            .await
            .unwrap()
            .rows_affected(),
        1
    );
    harness.run_until_resource_status("cleaned").await;
    let cleaned = pin_rows::resources(db).await.remove(0);
    assert_eq!((cleaned.handle, cleaned.action), (None, None));
    assert!(pin_rows::refs(db).await.is_empty());
    let calls = harness.pinata.received_requests().await.unwrap();
    assert_eq!(
        calls
            .iter()
            .map(|r| (r.method.as_str(), r.url.path()))
            .collect::<Vec<_>>(),
        [("POST", "/psa/pins"), ("DELETE", "/psa/pins/request-1")]
    );
    assert_signed_body(&harness, "copy.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}
