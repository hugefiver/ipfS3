use super::{
    ClusterAuth, ClusterPinset,
    provider::{
        ClusterFailureCode, ClusterProtocol, ClusterProvider, ClusterProviderOptions,
        ClusterStrategy,
    },
};
use crate::pinning::provider::SubmitPin;
use cid::Cid;
use serde_json::json;
use std::{collections::BTreeMap, time::Duration};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path, query_param},
};

pub(super) const RAW_EMPTY: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const CID_V0: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";

pub(super) fn options(
    endpoint: &str,
    protocol: ClusterProtocol,
    strategy: ClusterStrategy,
) -> ClusterProviderOptions {
    ClusterProviderOptions {
        protocol,
        strategy,
        proxy_endpoint: (protocol == ClusterProtocol::Proxy)
            .then(|| format!("{endpoint}/proxy/api/v0")),
        rest_endpoint: format!("{endpoint}/rest"),
        proxy_auth: (protocol == ClusterProtocol::Proxy)
            .then(|| ClusterAuth::Bearer("proxy-only".into())),
        rest_auth: Some(ClusterAuth::Bearer("rest-only".into())),
        pinset: ClusterPinset::new("cluster-test", "pinset").unwrap(),
        allow_private_network: true,
        ..ClusterProviderOptions::default()
    }
}

pub(super) fn request(cid: &str) -> SubmitPin {
    SubmitPin {
        cid: cid.into(),
        name: "object-name-sentinel".into(),
        metadata: BTreeMap::from([("private".into(), "metadata-sentinel".into())]),
    }
}

#[tokio::test]
async fn cid_submit_and_exact_handle_removal_use_only_the_configured_protocol() {
    let parsed: Cid = CID_V0.parse().unwrap();
    let canonical = Cid::new_v1(parsed.codec(), *parsed.hash()).to_string();
    for protocol in [ClusterProtocol::Proxy, ClusterProtocol::Rest] {
        let server = MockServer::start().await;
        let (submit, remove, response) = match protocol {
            ClusterProtocol::Proxy => (
                Mock::given(method("POST"))
                    .and(path("/proxy/api/v0/pin/add"))
                    .and(query_param("arg", &canonical))
                    .and(query_param("type", "recursive"))
                    .and(header("authorization", "Bearer proxy-only")),
                Mock::given(method("POST"))
                    .and(path("/proxy/api/v0/pin/rm"))
                    .and(query_param("arg", CID_V0))
                    .and(header("authorization", "Bearer proxy-only")),
                json!({"Pins": [CID_V0]}),
            ),
            ClusterProtocol::Rest => (
                Mock::given(method("POST"))
                    .and(path(format!("/rest/pins/{canonical}")))
                    .and(header("authorization", "Bearer rest-only")),
                Mock::given(method("DELETE"))
                    .and(path(format!("/rest/pins/{CID_V0}")))
                    .and(header("authorization", "Bearer rest-only")),
                json!({"cid": CID_V0}),
            ),
        };
        for route in [submit, remove] {
            route
                .respond_with(ResponseTemplate::new(200).set_body_json(&response))
                .expect(1)
                .mount(&server)
                .await;
        }
        let provider =
            ClusterProvider::new(options(&server.uri(), protocol, ClusterStrategy::Cid)).unwrap();
        let receipt = provider.submit(request(&canonical)).await.unwrap();
        assert_eq!(receipt.request_id, CID_V0);
        assert_eq!(receipt.resource.cid, canonical);
        provider.unpin(&receipt.request_id).await.unwrap();
        let requests = server.received_requests().await.unwrap();
        assert_eq!(
            requests.len(),
            2,
            "submission and removal each make exactly one request"
        );
        assert!(requests.iter().all(|request| request.body.is_empty()));
    }
}

#[tokio::test]
async fn native_rest_status_only_removal_ack_does_not_poll() {
    for status in [202, 204] {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path(format!("/rest/pins/{RAW_EMPTY}")))
            .respond_with(ResponseTemplate::new(status))
            .expect(1)
            .mount(&server)
            .await;
        let provider = ClusterProvider::new(options(
            &server.uri(),
            ClusterProtocol::Rest,
            ClusterStrategy::Cid,
        ))
        .unwrap();
        provider.unpin(RAW_EMPTY).await.unwrap();
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn lost_malformed_and_wrong_root_submit_responses_do_not_retry_or_cleanup() {
    let parsed: Cid = RAW_EMPTY.parse().unwrap();
    let other_codec = Cid::new_v1(0x70, *parsed.hash()).to_string();
    for (response, expected) in [
        (
            ResponseTemplate::new(307).insert_header("location", "/alternate"),
            ClusterFailureCode::RedirectRejected,
        ),
        (
            ResponseTemplate::new(403).set_body_string("private upstream detail"),
            ClusterFailureCode::HttpStatus,
        ),
        (
            ResponseTemplate::new(200).set_body_json(json!({"cid": "invalid"})),
            ClusterFailureCode::Protocol,
        ),
        (
            ResponseTemplate::new(200).set_body_json(json!({"cid": other_codec})),
            ClusterFailureCode::CidMismatch,
        ),
        (
            ResponseTemplate::new(200)
                .set_body_json(json!({"cid": RAW_EMPTY}))
                .insert_header("X-Stream-Error", "upstream finalization error"),
            ClusterFailureCode::Protocol,
        ),
        (
            ResponseTemplate::new(200)
                .set_body_json(json!({"cid": RAW_EMPTY}))
                .set_delay(Duration::from_secs(1)),
            ClusterFailureCode::IdleTimeout,
        ),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(format!("/rest/pins/{RAW_EMPTY}")))
            .respond_with(response)
            .expect(1)
            .mount(&server)
            .await;
        let mut opts = options(&server.uri(), ClusterProtocol::Rest, ClusterStrategy::Cid);
        opts.transfer_limits.idle_timeout = Duration::from_millis(250);
        let provider = ClusterProvider::new(opts).unwrap();
        let error = provider.submit(request(RAW_EMPTY)).await.unwrap_err();
        assert_eq!(error.code, expected);
        assert!(!error.to_string().contains("private upstream detail"));
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn proxy_removal_requires_one_matching_cid_and_rejects_invalid_handles_locally() {
    for (body, expected) in [
        (json!({"Pins": [CID_V0]}), ClusterFailureCode::CidMismatch),
        (
            json!({"Pins": [RAW_EMPTY, CID_V0]}),
            ClusterFailureCode::Protocol,
        ),
        (json!({"cid": RAW_EMPTY}), ClusterFailureCode::Protocol),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/proxy/api/v0/pin/rm"))
            .and(query_param("arg", RAW_EMPTY))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1)
            .mount(&server)
            .await;
        let provider = ClusterProvider::new(options(
            &server.uri(),
            ClusterProtocol::Proxy,
            ClusterStrategy::Cid,
        ))
        .unwrap();
        assert_eq!(
            provider.unpin("invalid-handle").await.unwrap_err().code,
            ClusterFailureCode::InvalidCid
        );
        assert!(server.received_requests().await.unwrap().is_empty());
        assert_eq!(provider.unpin(RAW_EMPTY).await.unwrap_err().code, expected);
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}
