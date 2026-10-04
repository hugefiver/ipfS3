use super::tests::{CID, local, provider, query, recursive, source_file, submit};
use super::*;
use crate::pinning::provider::PinningProvider;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn zero_length_stored_file_and_v0_v1_roots_are_accepted() {
    for requested in [CID, "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE"] {
        let target = MockServer::start().await;
        let source = MockServer::start().await;
        let canonical = cid::canonical(requested).unwrap();
        source_file(&source, requested, b"").await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(format!("{{\"Hash\":\"{canonical}\"}}\n")),
            )
            .mount(&target)
            .await;
        recursive(&target, &canonical).await;
        let p = provider(&target, &source, RpcProfile::Filebase, RpcStrategy::Upload);
        let pin = p.submit(submit(requested)).await.unwrap();
        assert!(cid::equivalent(&pin.cid, requested).unwrap());
        assert_eq!(
            pin.request_id,
            p.find(query(&canonical)).await.unwrap()[0].request_id
        );
        let requests = target.received_requests().await.unwrap();
        let add = requests
            .iter()
            .find(|r| r.url.path() == "/api/v0/add")
            .unwrap();
        let expected_version = if requested.starts_with("Qm") {
            "0"
        } else {
            "1"
        };
        assert!(
            add.url
                .query_pairs()
                .any(|(key, value)| key == "cid-version" && value == expected_version)
        );
    }
}

#[tokio::test]
async fn kubo_upload_pins_only_after_matching_root_and_verifies_local_dag() {
    let target = MockServer::start().await;
    let source = MockServer::start().await;
    source_file(&source, CID, b"stored bytes").await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .and(query_param("pin", "false"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "{{\"Bytes\":5}}\n{{\"Bytes\":12}}\n{{\"Hash\":\"{CID}\",\"Size\":\"12\"}}\n"
        )))
        .mount(&target)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins": [CID]})))
        .expect(1)
        .mount(&target)
        .await;
    local(&target, CID).await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Upload);
    p.submit(submit(CID)).await.unwrap();
    let requests = target.received_requests().await.unwrap();
    let commands: Vec<_> = requests.iter().map(|r| r.url.path()).collect();
    assert_eq!(&commands[..2], ["/api/v0/add", "/api/v0/pin/add"]);
}

#[tokio::test]
async fn basic_auth_is_applied_to_target_controls_without_leaking_to_source() {
    let target = MockServer::start().await;
    let source = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(header("authorization", "Basic dXNlcjpwYXNzd29yZA=="))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins": [CID]})))
        .mount(&target)
        .await;
    local(&target, CID).await;
    let p = IpfsRpcProvider::new(
        "remote".into(),
        format!("{}/api/v0///", target.uri()),
        KuboClient::new(source.uri()),
        RpcProfile::Kubo,
        RpcStrategy::Cid,
        Some(RpcAuth::Basic {
            username: "user".into(),
            password: "password".into(),
        }),
    )
    .unwrap();
    p.submit(submit(CID)).await.unwrap();
    assert!(
        target.received_requests().await.unwrap().iter().all(|r| r
            .headers
            .get("authorization")
            .unwrap()
            == "Basic dXNlcjpwYXNzd29yZA==")
    );
    assert!(source.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn malformed_add_and_postdispatch_auth_statuses_keep_unknown_effect() {
    for response in [
        ResponseTemplate::new(200).set_body_string("{}\n"),
        ResponseTemplate::new(200)
            .set_body_string(format!("{{\"Hash\":\"{CID}\"}}\n{{\"Bytes\":0}}\n")),
        ResponseTemplate::new(200)
            .set_body_string(format!("{{\"Hash\":\"{CID}\"}}\n{{\"Hash\":\"{CID}\"}}\n")),
        ResponseTemplate::new(200).set_body_string("{\"Hash\":\"invalid-cid\"}\n"),
        ResponseTemplate::new(400),
        ResponseTemplate::new(401),
        ResponseTemplate::new(403),
        ResponseTemplate::new(429),
    ] {
        let target = MockServer::start().await;
        let source = MockServer::start().await;
        source_file(&source, CID, b"stored").await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(response)
            .mount(&target)
            .await;
        let p = provider(&target, &source, RpcProfile::Filebase, RpcStrategy::Upload);
        let error = p.submit(submit(CID)).await.unwrap_err();
        assert!(
            !error.definitely_not_submitted(),
            "postdispatch effect must remain unknown: {error:?}"
        );
    }
}

#[tokio::test]
async fn source_initial_error_and_redirect_fail_before_any_target_mutation() {
    for response in [
        ResponseTemplate::new(200).insert_header("X-Stream-Error", "private failure"),
        ResponseTemplate::new(307).insert_header("Location", "http://127.0.0.1:1/private"),
    ] {
        let target = MockServer::start().await;
        let source = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/files/stat"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"Hash": CID, "Type": "file"})),
            )
            .mount(&source)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(response)
            .mount(&source)
            .await;
        let p = provider(&target, &source, RpcProfile::Filebase, RpcStrategy::Upload);
        let error = p.submit(submit(CID)).await.unwrap_err();
        assert!(error.definitely_not_submitted());
        assert!(!format!("{error:?}").contains("private"));
        assert!(target.received_requests().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn partial_or_paged_pin_queries_are_not_complete_absence() {
    for response in [
        ResponseTemplate::new(206).set_body_json(serde_json::json!({"Keys": {}})),
        ResponseTemplate::new(200)
            .insert_header("Content-Range", "bytes 0-10/100")
            .set_body_json(serde_json::json!({"Keys": {}})),
        ResponseTemplate::new(200)
            .set_body_json(serde_json::json!({"Keys": {}, "Next": "page-two"})),
    ] {
        let target = MockServer::start().await;
        let source = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/ls"))
            .respond_with(response)
            .mount(&target)
            .await;
        let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Cid);
        assert!(matches!(
            p.observe(query(CID)).await,
            QueryObservation::Unknown(_)
        ));
    }
}

#[tokio::test]
async fn remote_ref_never_grants_managed_ownership() {
    use crate::pinning::identity::{CleanupMode, Ownership, ProviderRouteSnapshot};
    let target = MockServer::start().await;
    let source = MockServer::start().await;
    local(&target, CID).await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Cid);
    let pin = p.find(query(CID)).await.unwrap().remove(0);
    let route = ProviderRouteSnapshot {
        provider_id: "target".into(),
        backend: "kubo".into(),
        scope: "node".into(),
        storage_domain: "target-node".into(),
        credential_revision: 1,
        endpoint_revision: 1,
        secret_ref: Some("env:RPC_TOKEN".into()),
        api_profile: "kubo".into(),
        strategy: "cid".into(),
        cleanup: CleanupMode::Managed,
    };
    let reference = p.remote_ref(&pin, route).unwrap();
    assert_eq!(reference.ownership, Ownership::Unknown);
    assert_eq!(
        reference.resource_type,
        crate::pinning::identity::RemoteResourceType::RpcPin
    );
}
