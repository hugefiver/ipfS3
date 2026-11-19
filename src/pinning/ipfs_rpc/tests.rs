use super::*;
use crate::pinning::provider::{PinningProvider, ProviderErrorClass, RemotePinStatus, SubmitPin};
use std::collections::BTreeMap;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub(super) const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
pub(super) const OTHER: &str = "bafkreigh2akiscaildc6ii5zji4bq7kly5k3s7svv6q2wx2nn5rtj5xuu4";

pub(super) fn submit(cid: &str) -> SubmitPin {
    SubmitPin {
        cid: cid.into(),
        name: "object".into(),
        metadata: BTreeMap::new(),
    }
}

pub(super) fn provider(
    target: &MockServer,
    source: &MockServer,
    profile: RpcProfile,
    strategy: RpcStrategy,
) -> IpfsRpcProvider {
    IpfsRpcProvider::new(
        "remote".into(),
        target.uri(),
        KuboClient::new(source.uri()),
        profile,
        strategy,
        Some(RpcAuth::Bearer("target-secret".into())),
    )
    .unwrap()
}

pub(super) async fn source_file(server: &MockServer, cid: &str, bytes: &[u8]) {
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"Hash": cid, "Type": "file"})),
        )
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(bytes))
        .mount(server)
        .await;
}

#[test]
fn auth_debug_is_redacted_and_capability_matrix_is_closed() {
    let auth = RpcAuth::Basic {
        username: "private-user".into(),
        password: "private-password".into(),
    };
    let debug = format!("{auth:?} {:?}", RpcAuth::Bearer("private-token".into()));
    assert!(!debug.contains("private"));
    assert!(RpcProfile::Filebase.validate(RpcStrategy::Cid).is_err());
    assert!(RpcProfile::Filebase.validate(RpcStrategy::Car).is_err());
    assert!(RpcProfile::Filebase.validate(RpcStrategy::Upload).is_ok());
}

#[test]
fn cid_identity_includes_codec_but_accepts_v0_v1_equivalence() {
    let v0 = ::cid::Cid::try_from("QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE").unwrap();
    let v1 = ::cid::Cid::new_v1(v0.codec(), *v0.hash());
    assert!(cid::equivalent(&v0, &v1));
    let raw = ::cid::Cid::new_v1(0x55, *v0.hash());
    assert!(!cid::equivalent(&raw, &v1));
}

#[tokio::test]
async fn filebase_uses_its_add_and_pin_parameters_without_followup_requests() {
    let target = MockServer::start().await;
    let source = MockServer::start().await;
    let stored = b"\0encrypted-stored-bytes\xff";
    source_file(&source, CID, stored).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .and(header("authorization", "Bearer target-secret"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{CID}\"}}\n")),
        )
        .expect(1)
        .mount(&target)
        .await;
    let p = provider(&target, &source, RpcProfile::Filebase, RpcStrategy::Upload);
    let remote = p.submit(submit(CID)).await.unwrap();
    assert_eq!(remote.status, RemotePinStatus::Pinned);
    assert_eq!(remote.cid, CID);
    assert_eq!(remote.request_id, format!("rpc-pin:filebase:{CID}"));
    let requests = target.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    let add = &requests[0];
    let keys: Vec<_> = add.url.query_pairs().map(|(k, _)| k.into_owned()).collect();
    assert_eq!(keys, ["cid-version", "wrap-with-directory"]);
    assert!(add.body.windows(stored.len()).any(|w| w == stored));
    assert!(
        source
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| !r.headers.contains_key("authorization"))
    );
}

#[tokio::test]
async fn upload_mismatch_does_not_retry_or_remove_the_returned_root() {
    let target = MockServer::start().await;
    let source = MockServer::start().await;
    source_file(&source, CID, b"stored").await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{OTHER}\"}}\n")),
        )
        .mount(&target)
        .await;
    let p = provider(&target, &source, RpcProfile::Filebase, RpcStrategy::Upload);
    let error = p.submit(submit(CID)).await.unwrap_err();
    assert_eq!(error.class, ProviderErrorClass::Protocol);
    assert!(!error.definitely_not_submitted());
    assert!(!format!("{error:?}").contains("target-secret"));
    let requests = target.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].url.path(), "/api/v0/add");
}

#[tokio::test]
async fn exact_recursive_pin_add_ack_completes_kubo_cid_submit() {
    let target = MockServer::start().await;
    let source = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("arg", CID))
        .and(query_param("recursive", "true"))
        .and(query_param("progress", "false"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins": [CID]})))
        .expect(1)
        .mount(&target)
        .await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Cid);
    assert_eq!(
        p.submit(submit(CID)).await.unwrap().status,
        RemotePinStatus::Pinned
    );
    assert_eq!(target.received_requests().await.unwrap().len(), 1);
    assert!(source.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn pin_add_needs_a_single_matching_cid() {
    let path_cid = format!("/ipfs/{CID}");
    let mut bytes = cid::parse(CID).unwrap().to_bytes();
    bytes.push(0);
    let trailing_bytes = ::cid::multibase::encode(::cid::multibase::Base::Base32Lower, bytes);
    for pins in [
        vec![],
        vec![OTHER],
        vec!["invalid-cid"],
        vec![path_cid.as_str()],
        vec![trailing_bytes.as_str()],
        vec![CID, CID],
    ] {
        let target = MockServer::start().await;
        let source = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins": pins})),
            )
            .expect(1)
            .mount(&target)
            .await;
        let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Cid);
        assert_eq!(
            p.submit(submit(CID)).await.unwrap_err().class,
            ProviderErrorClass::Protocol
        );
        assert_eq!(target.received_requests().await.unwrap().len(), 1);
    }
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Cid);
    assert!(
        p.submit(submit("invalid-cid"))
            .await
            .unwrap_err()
            .definitely_not_submitted()
    );
    assert!(target.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn directory_upload_is_rejected_before_cat_or_target_writes() {
    let target = MockServer::start().await;
    let source = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"Hash": CID, "Type": "directory"})),
        )
        .mount(&source)
        .await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Upload);
    let error = p.submit(submit(CID)).await.unwrap_err();
    assert!(error.definitely_not_submitted());
    assert!(target.received_requests().await.unwrap().is_empty());
    assert!(
        source
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| r.url.path() != "/api/v0/cat")
    );
}

#[tokio::test]
async fn car_uses_full_export_and_completes_from_import_pin_ack() {
    let target = MockServer::start().await;
    let source = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/export"))
        .and(query_param("arg", CID))
        .and(query_param("offline", "false"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"mock complete CAR"))
        .expect(1)
        .mount(&source)
        .await;
    Mock::given(method("POST")).and(path("/api/v0/dag/import"))
        .and(query_param("pin-roots", "true"))
        .and(query_param("stats", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!("{{\"Root\":{{\"Cid\":{{\"/\":\"{CID}\"}},\"PinErrorMsg\":\"\"}}}}\n{{\"Stats\":{{\"BlockCount\":1,\"BlockBytesCount\":17}}}}\n")))
        .expect(1).mount(&target).await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Car);
    assert_eq!(
        p.submit(submit(CID)).await.unwrap().status,
        RemotePinStatus::Pinned
    );
    assert_eq!(source.received_requests().await.unwrap().len(), 1);
    assert_eq!(target.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn redirects_do_not_forward_credentials_or_become_success() {
    let target = MockServer::start().await;
    let elsewhere = MockServer::start().await;
    let source = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(
            ResponseTemplate::new(307)
                .insert_header("Location", format!("{}/api/v0/pin/add", elsewhere.uri())),
        )
        .mount(&target)
        .await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Cid);
    let error = p.submit(submit(CID)).await.unwrap_err();
    assert!(!error.definitely_not_submitted());
    assert!(elsewhere.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn malformed_or_foreign_handles_are_rejected_without_a_post() {
    let target = MockServer::start().await;
    let source = MockServer::start().await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Cid);
    for handle in [
        CID.to_owned(),
        format!("rpc-pin:filebase:{CID}"),
        "rpc-pin:kubo:not-a-cid".into(),
        format!("rpc-pin:kubo:{CID}/child"),
    ] {
        assert_eq!(
            p.unpin(&handle).await.unwrap_err().class,
            ProviderErrorClass::InvalidInput
        );
    }
    assert!(target.received_requests().await.unwrap().is_empty());
}
