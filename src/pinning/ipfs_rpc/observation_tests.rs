use super::tests::{CID, OTHER, provider, source_file, submit};
use super::*;
use crate::pinning::{identity::Ownership, provider::PinningProvider};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn mismatch_keeps_actual_clean_root_outside_error_text() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    source_file(&source, CID, b"stored").await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{OTHER}\"}}\n")),
        )
        .mount(&target)
        .await;
    let p = provider(&target, &source, RpcProfile::Filebase, RpcStrategy::Upload);
    let observation = p.submit_observed(submit(CID)).await;
    let error = observation.result.unwrap_err();
    assert!(!error.message.contains(CID));
    assert!(!error.message.contains(OTHER));
    assert!(!error.definitely_not_submitted());
    assert_eq!(observation.effect, RpcSubmitEffect::Observed);
    assert_eq!(observation.resources.len(), 1);
    let resource = &observation.resources[0];
    assert_eq!(resource.cid, OTHER);
    assert_eq!(resource.status, RpcResourceStatus::PinAccepted);
    assert_eq!(resource.ownership, Ownership::Unknown);
    assert_eq!(
        resource.resource_type,
        crate::pinning::identity::RemoteResourceType::RpcPin
    );
    assert!(!resource.request_id.contains(&target.uri()));
    assert!(
        target
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| r.url.path() != "/api/v0/pin/rm")
    );
}

#[tokio::test]
async fn multi_root_cid_submit_keeps_every_observed_pin_but_never_expected_pinned() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins": [CID, OTHER]})),
        )
        .mount(&target)
        .await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Cid);
    let observation = p.submit_observed(submit(CID)).await;
    assert!(observation.result.is_err());
    assert_eq!(observation.effect, RpcSubmitEffect::Observed);
    assert_eq!(observation.resources.len(), 2);
    assert!(
        observation.resources.iter().all(
            |r| r.status == RpcResourceStatus::PinAccepted && r.ownership == Ownership::Unknown
        )
    );
    assert!(p.submit(submit(CID)).await.is_err());
    assert!(
        target
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| r.url.path() == "/api/v0/pin/add")
    );
}

#[tokio::test]
async fn multi_root_and_pin_error_car_responses_keep_structured_resources() {
    for (roots, expected_status) in [
        (
            format!(
                "{{\"Root\":{{\"Cid\":{{\"/\":\"{CID}\"}},\"PinErrorMsg\":\"\"}}}}\n{{\"Root\":{{\"Cid\":{{\"/\":\"{OTHER}\"}},\"PinErrorMsg\":\"\"}}}}\n"
            ),
            RpcResourceStatus::PinAccepted,
        ),
        (
            format!(
                "{{\"Root\":{{\"Cid\":{{\"/\":\"{OTHER}\"}},\"PinErrorMsg\":\"private failure\"}}}}\n"
            ),
            RpcResourceStatus::PinError,
        ),
    ] {
        let source = MockServer::start().await;
        let target = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/dag/export"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"mock CAR"))
            .mount(&source)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/dag/import"))
            .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                "{roots}{{\"Stats\":{{\"BlockCount\":2,\"BlockBytesCount\":8}}}}\n"
            )))
            .mount(&target)
            .await;
        let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Car);
        let observation = p.submit_observed(submit(CID)).await;
        let error = observation.result.unwrap_err();
        assert!(!error.message.contains("private failure"));
        assert!(!error.message.contains(OTHER));
        assert_eq!(observation.effect, RpcSubmitEffect::Observed);
        assert!(observation.resources.iter().any(|r| r.cid == OTHER));
        assert!(
            observation
                .resources
                .iter()
                .all(|r| r.status == expected_status && r.ownership == Ownership::Unknown)
        );
        assert!(
            target
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|r| r.url.path() == "/api/v0/dag/import")
        );
    }
}

#[tokio::test]
async fn later_unknown_write_keeps_prior_stored_root() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    source_file(&source, CID, b"stored").await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{CID}\"}}\n")),
        )
        .mount(&target)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&target)
        .await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Upload);
    let observation = p.submit_observed(submit(CID)).await;
    assert!(observation.result.is_err());
    assert_eq!(observation.effect, RpcSubmitEffect::Unknown);
    assert_eq!(observation.resources.len(), 1);
    assert_eq!(observation.resources[0].cid, CID);
    assert_eq!(observation.resources[0].status, RpcResourceStatus::Stored);
}

#[tokio::test]
async fn truncated_io_without_clean_root_is_explicitly_unknown() {
    let source = MockServer::start().await;
    let (endpoint, server) = super::stream_tests::raw_response(
        b"HTTP/1.1 200 OK\r\nContent-Length: 999\r\nConnection: close\r\n\r\n{\"Pins\":[".to_vec(),
        false,
    )
    .await;
    let p = IpfsRpcProvider::new(
        "remote".into(),
        endpoint,
        KuboClient::new(source.uri()),
        RpcProfile::Kubo,
        RpcStrategy::Cid,
        None,
    )
    .unwrap();
    let observation = p.submit_observed(submit(CID)).await;
    assert!(observation.result.is_err());
    assert_eq!(observation.effect, RpcSubmitEffect::Unknown);
    assert!(observation.resources.is_empty());
    server.await.unwrap();
}

#[tokio::test]
async fn observed_resource_vector_is_bounded_and_duplicate_roots_still_fail() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    let roots: Vec<_> = (0..MAX_OBSERVED_RESOURCES + 2)
        .map(|index| {
            let base = ::cid::Cid::try_from(CID).unwrap();
            ::cid::Cid::new_v1(0x1000 + index as u64, *base.hash()).to_string()
        })
        .collect();
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins": roots})))
        .mount(&target)
        .await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Cid);
    let observation = p.submit_observed(submit(CID)).await;
    assert!(observation.result.is_err());
    assert_eq!(observation.resources.len(), MAX_OBSERVED_RESOURCES);
    assert_eq!(observation.effect, RpcSubmitEffect::Unknown);
    assert!(
        observation
            .resources
            .iter()
            .all(|r| r.ownership == Ownership::Unknown)
    );
    let duplicate_target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins": [CID, CID]})),
        )
        .mount(&duplicate_target)
        .await;
    let p = provider(
        &duplicate_target,
        &source,
        RpcProfile::Kubo,
        RpcStrategy::Cid,
    );
    let duplicate = p.submit_observed(submit(CID)).await;
    assert!(duplicate.result.is_err());
    assert_eq!(duplicate.resources.len(), 1);
    assert_eq!(
        duplicate.resources[0].status,
        RpcResourceStatus::PinAccepted
    );
}

#[tokio::test]
async fn successful_observation_is_recursive_verified_but_ownership_stays_unknown() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins": [CID]})))
        .mount(&target)
        .await;
    super::tests::local(&target, CID).await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Cid);
    let observation = p.submit_observed(submit(CID)).await;
    assert_eq!(
        observation.result.unwrap().status,
        crate::pinning::provider::RemotePinStatus::Pinned
    );
    assert_eq!(observation.effect, RpcSubmitEffect::Observed);
    assert_eq!(observation.resources.len(), 1);
    assert_eq!(
        observation.resources[0].status,
        RpcResourceStatus::RecursiveVerified
    );
    assert_eq!(observation.resources[0].ownership, Ownership::Unknown);
}

#[tokio::test]
async fn invalid_local_input_is_explicitly_not_submitted_without_resources() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Cid);
    let observation = p.submit_observed(submit("invalid CID")).await;
    assert!(observation.result.unwrap_err().definitely_not_submitted());
    assert_eq!(observation.effect, RpcSubmitEffect::NotSubmitted);
    assert!(observation.resources.is_empty());
    assert!(target.received_requests().await.unwrap().is_empty());
}
