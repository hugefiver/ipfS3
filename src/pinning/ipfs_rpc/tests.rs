use super::*;
use crate::pinning::provider::{FindPin, PinningProvider, RemotePinStatus, SubmitPin};
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

pub(super) fn query(cid: &str) -> FindPin {
    FindPin {
        cid: cid.into(),
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

pub(super) async fn recursive(server: &MockServer, cid: &str) {
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/ls"))
        .and(query_param("arg", cid))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"Keys": {(cid): {"Type": "recursive"}}})),
        )
        .mount(server)
        .await;
}

pub(super) async fn local(server: &MockServer, cid: &str) {
    recursive(server, cid).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID": "node-target"})),
        )
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .and(query_param("arg", format!("/ipfs/{cid}")))
        .and(query_param("offline", "true"))
        .and(query_param("with-local", "true"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"Hash": cid, "WithLocality": true, "Local": true}),
            ),
        )
        .mount(server)
        .await;
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
    assert!(cid::equivalent(&v0.to_string(), &v1.to_string()).unwrap());
    let raw = ::cid::Cid::new_v1(0x55, *v0.hash());
    assert!(!cid::equivalent(&raw.to_string(), &v1.to_string()).unwrap());
}

#[tokio::test]
async fn filebase_only_uses_official_add_and_pin_ls_parameters_and_stored_bytes() {
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
    recursive(&target, CID).await;
    let p = provider(&target, &source, RpcProfile::Filebase, RpcStrategy::Upload);
    let remote = p.submit(submit(CID)).await.unwrap();
    assert_eq!(remote.status, RemotePinStatus::Pinned);
    assert!(!remote.request_id.contains(&target.uri()));
    let requests = target.received_requests().await.unwrap();
    let add = requests
        .iter()
        .find(|r| r.url.path() == "/api/v0/add")
        .unwrap();
    let keys: Vec<_> = add.url.query_pairs().map(|(k, _)| k.into_owned()).collect();
    assert_eq!(keys, ["cid-version", "wrap-with-directory"]);
    assert!(add.body.windows(stored.len()).any(|w| w == stored));
    assert!(!requests.iter().any(|r| r.url.path() == "/api/v0/pin/add"));
    let ls = requests
        .iter()
        .find(|r| r.url.path() == "/api/v0/pin/ls")
        .unwrap();
    assert!(
        ls.url
            .query_pairs()
            .all(|(k, _)| matches!(k.as_ref(), "arg" | "stream" | "names"))
    );
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
async fn mismatch_preserves_unknown_ownership_and_never_unpins() {
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
    let observation = p.submit_observed(submit(CID)).await;
    let error = observation.result.unwrap_err();
    let evidence = &observation.resources[0];
    assert_eq!(evidence.cid, OTHER);
    assert_eq!(
        evidence.ownership,
        crate::pinning::identity::Ownership::Unknown
    );
    assert!(!error.definitely_not_submitted());
    assert!(!format!("{error:?}").contains("target-secret"));
    assert!(
        !target
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|r| r.url.path() == "/api/v0/pin/rm")
    );
}

#[tokio::test]
async fn recursive_local_proof_is_required_for_kubo_cid_submit() {
    let target = MockServer::start().await;
    let source = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("recursive", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins": [CID]})))
        .mount(&target)
        .await;
    local(&target, CID).await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Cid);
    assert_eq!(
        p.submit(submit(CID)).await.unwrap().status,
        RemotePinStatus::Pinned
    );
    assert!(source.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn direct_indirect_and_failed_locality_are_unknown_not_complete_absence() {
    for pin_type in ["direct", "indirect", "future-type"] {
        let target = MockServer::start().await;
        let source = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/ls"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"Keys": {(CID): {"Type": pin_type}}})),
            )
            .mount(&target)
            .await;
        let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Cid);
        assert!(matches!(
            p.observe(query(CID)).await,
            QueryObservation::Unknown(_)
        ));
        assert!(p.find(query(CID)).await.is_err());
    }
    let target = MockServer::start().await;
    let source = MockServer::start().await;
    recursive(&target, CID).await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Cid);
    assert!(matches!(
        p.observe(query(CID)).await,
        QueryObservation::Unknown(_)
    ));
}

#[tokio::test]
async fn only_full_authoritative_absence_is_complete() {
    let target = MockServer::start().await;
    let source = MockServer::start().await;
    Mock::given(method("POST")).and(path("/api/v0/pin/ls"))
        .respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({"Message": format!("path '{CID}' is not pinned"), "Code": 0, "Type": "error"})))
        .mount(&target).await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Cid);
    assert!(
        matches!(p.observe(query(CID)).await, QueryObservation::Complete(pins) if pins.is_empty())
    );
    for status in [401, 403, 404, 500] {
        let unknown = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/ls"))
            .respond_with(ResponseTemplate::new(status).set_body_string("private backend failure"))
            .mount(&unknown)
            .await;
        let p = provider(&unknown, &source, RpcProfile::Kubo, RpcStrategy::Cid);
        assert!(matches!(
            p.observe(query(CID)).await,
            QueryObservation::Unknown(_)
        ));
    }
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
async fn car_uses_full_export_and_requires_single_root_terminal_stats_and_local_proof() {
    let target = MockServer::start().await;
    let source = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/export"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"mock complete CAR"))
        .expect(1)
        .mount(&source)
        .await;
    Mock::given(method("POST")).and(path("/api/v0/dag/import"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!("{{\"Root\":{{\"Cid\":{{\"/\":\"{CID}\"}},\"PinErrorMsg\":\"\"}}}}\n{{\"Stats\":{{\"BlockCount\":1,\"BlockBytesCount\":17}}}}\n")))
        .expect(1).mount(&target).await;
    local(&target, CID).await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Car);
    assert_eq!(
        p.submit(submit(CID)).await.unwrap().status,
        RemotePinStatus::Pinned
    );
    let exports = source.received_requests().await.unwrap();
    assert!(
        exports[0]
            .url
            .query_pairs()
            .all(|(k, v)| !(k == "offline" && v == "true") && !(k == "local-only" && v == "true"))
    );
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
async fn historical_routes_and_typed_ids_do_not_depend_on_current_strategy_or_endpoint() {
    let target = MockServer::start().await;
    let source = MockServer::start().await;
    local(&target, CID).await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Upload);
    let pins = p.find_historical(query(CID), "rpc", "car").await.unwrap();
    assert_eq!(
        p.get_historical(&pins[0].request_id, "rpc", "cid")
            .await
            .unwrap()
            .cid,
        CID
    );
    assert!(p.get(CID).await.is_err());
    assert!(
        p.find_historical(query(CID), "cluster", "car")
            .await
            .is_err()
    );
    let foreign = pins[0].request_id.replace("kubo", "filebase");
    assert!(p.unpin(&foreign).await.is_err());
    assert!(
        target
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| r.url.path() != "/api/v0/pin/rm")
    );
}
