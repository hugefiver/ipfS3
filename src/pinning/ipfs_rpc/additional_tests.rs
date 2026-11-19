use super::tests::{CID, provider, source_file, submit};
use super::*;
use crate::pinning::provider::{PinningProvider, RemotePinStatus};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn zero_length_stored_file_and_v0_v1_roots_are_accepted() {
    for (requested, version) in [
        (CID, "1"),
        ("QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE", "0"),
    ] {
        let target = MockServer::start().await;
        let source = MockServer::start().await;
        let canonical = cid::canonical(&cid::parse(requested).unwrap()).to_string();
        source_file(&source, requested, b"").await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .and(query_param("cid-version", version))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(format!("{{\"Hash\":\"{canonical}\"}}\n")),
            )
            .expect(1)
            .mount(&target)
            .await;
        let p = provider(&target, &source, RpcProfile::Filebase, RpcStrategy::Upload);
        let pin = p.submit(submit(requested)).await.unwrap();
        assert_eq!(pin.cid, canonical);
        assert_eq!(pin.request_id, format!("rpc-pin:filebase:{canonical}"));
    }
}

#[tokio::test]
async fn kubo_upload_and_unpin_preserve_the_acknowledged_cid_version() {
    let v0 = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
    let v1 = cid::canonical(&cid::parse(v0).unwrap()).to_string();
    let target = MockServer::start().await;
    let source = MockServer::start().await;
    source_file(&source, &v1, b"stored bytes").await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .and(query_param("cid-version", "0"))
        .and(query_param("pin", "true"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{v0}\"}}\n")),
        )
        .expect(1)
        .mount(&target)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/rm"))
        .and(query_param("arg", v0))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins": [v0]})))
        .expect(1)
        .mount(&target)
        .await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Upload);
    let remote = p.submit(submit(v0)).await.unwrap();
    assert_eq!(remote.cid, v1);
    assert_eq!(remote.request_id, format!("rpc-pin:kubo:{v0}"));
    p.unpin(&remote.request_id).await.unwrap();
    assert_eq!(target.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn kubo_upload_pins_in_the_add_request_and_needs_no_followup() {
    let target = MockServer::start().await;
    let source = MockServer::start().await;
    source_file(&source, CID, b"stored bytes").await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .and(query_param("pin", "true"))
        .and(query_param("progress", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "{{\"Bytes\":5}}\n{{\"Bytes\":12}}\n{{\"Hash\":\"{CID}\",\"Size\":\"12\"}}\n"
        )))
        .expect(1)
        .mount(&target)
        .await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Upload);
    let remote = p.submit(submit(CID)).await.unwrap();
    assert_eq!(remote.status, RemotePinStatus::Pinned);
    assert_eq!(remote.cid, CID);
    assert_eq!(target.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn basic_auth_is_applied_to_target_controls_without_leaking_to_source() {
    let target = MockServer::start().await;
    let source = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(header("authorization", "Basic dXNlcjpwYXNzd29yZA=="))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins": [CID]})))
        .expect(1)
        .mount(&target)
        .await;
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
    assert!(source.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn malformed_add_and_postdispatch_rejections_do_not_prove_nonsubmission() {
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
        ResponseTemplate::new(503),
    ] {
        let target = MockServer::start().await;
        let source = MockServer::start().await;
        source_file(&source, CID, b"stored").await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(response)
            .expect(1)
            .mount(&target)
            .await;
        let p = provider(&target, &source, RpcProfile::Filebase, RpcStrategy::Upload);
        let error = p.submit(submit(CID)).await.unwrap_err();
        assert!(!error.definitely_not_submitted(), "{error:?}");
        assert_eq!(target.received_requests().await.unwrap().len(), 1);
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
