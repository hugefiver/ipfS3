//! New transport contracts only; no repeat of the unchanged verifier matrix.
//! DTOs match pinned v1.1.6 source, without fixture-side protocol translation.
use crate::{car, cluster, pinning, stored_crypto};
use cluster::provider::{
    ClusterFailureCode, ClusterProtocol, ClusterProvider, ClusterProviderOptions,
    ClusterResourceStatus, ClusterSource, ClusterStrategy, ClusterWriteEffect,
};
use cluster::{ClusterPinset, ReplicaOutcome};
use pinning::{identity::Ownership, provider::SubmitPin};
use serde_json::json;
use std::collections::BTreeMap;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

fn request(cid: &str) -> SubmitPin {
    SubmitPin {
        cid: cid.into(),
        name: "private-name-not-sent".into(),
        metadata: BTreeMap::new(),
    }
}

fn options(
    source: &str,
    target: &str,
    protocol: ClusterProtocol,
    strategy: ClusterStrategy,
    stream: bool,
) -> ClusterProviderOptions {
    ClusterProviderOptions {
        protocol,
        strategy,
        stream_channels: stream,
        source: Some(ClusterSource {
            endpoint: source.into(),
            auth: None,
        }),
        rest_endpoint: target.into(),
        proxy_endpoint: (protocol == ClusterProtocol::Proxy).then(|| target.into()),
        allow_private_network: true,
        pinset: ClusterPinset::new("qa-contract", "one-pinset").unwrap(),
        ..ClusterProviderOptions::default()
    }
}

async fn add(target: &MockServer, protocol: ClusterProtocol, cid: &str, stream: bool) {
    let output = match protocol {
        ClusterProtocol::Proxy => json!({"Name":"content", "Hash":cid, "Size":"5"}),
        ClusterProtocol::Rest => json!({"name":"content", "cid":cid, "size":5}),
    };
    let mut reply = ResponseTemplate::new(200);
    if stream {
        reply = reply
            .insert_header("X-Chunked-Output", "1")
            .set_body_raw(format!("{output}\n"), "application/json");
    } else {
        reply = reply.set_body_json(json!([output]));
    }
    Mock::given(method("POST"))
        .and(path(if protocol == ClusterProtocol::Proxy {
            "/api/v0/add"
        } else {
            "/add"
        }))
        .respond_with(reply)
        .mount(target)
        .await;
}

#[tokio::test]
async fn cluster_provider_real_contract_car_negatives_preserve_unknown_without_fallback() {
    let child = car::raw_cid(b"child", 0x55);
    let (root, block) = car::directory(&child);
    let records = [(root.clone(), block), (child, b"child".to_vec())];
    for protocol in [ClusterProtocol::Proxy, ClusterProtocol::Rest] {
        for (case, bytes) in car::invalid_cars(&root, &records) {
            let source = MockServer::start().await;
            let target = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/v0/dag/export"))
                .respond_with(ResponseTemplate::new(200).set_body_bytes(bytes))
                .expect(1)
                .mount(&source)
                .await;
            add(&target, protocol, &root, true).await;
            let out = ClusterProvider::new(options(
                &source.uri(),
                &target.uri(),
                protocol,
                ClusterStrategy::Car,
                true,
            ))
            .unwrap()
            .submit_observed(request(&root))
            .await;
            assert_eq!(
                out.result.unwrap_err().code,
                ClusterFailureCode::InvalidCar,
                "{protocol:?}/{case}"
            );
            assert_eq!(out.effect, ClusterWriteEffect::Unknown, "{case}");
            assert!(out.replicas.is_none());
            assert!(out.resources.iter().any(|r| r.resource.cid == root));
            assert!(
                out.resources
                    .iter()
                    .all(|r| r.ownership == Ownership::Unknown)
            );
            let writes = target.received_requests().await.unwrap();
            assert!(
                writes.len() <= 1,
                "fallback or Poll after invalid CAR: {case}"
            );
            assert!(writes.iter().all(|r| {
                r.method == "POST"
                    && r.url
                        .query_pairs()
                        .any(|(k, v)| k == "format" && v == "car")
            }));
            assert_eq!(source.received_requests().await.unwrap().len(), 1);
        }
    }
}

#[tokio::test]
async fn cluster_provider_real_contract_crypto_upload_and_mismatch_never_retry_car_or_cid() {
    // Below the default UnixFS chunk size: this is genuinely a raw-leaf import
    // root, not a mock claiming that a multi-block upload returned a raw CID.
    // The real phase separately verifies multi-chunk encrypted files/DAGs.
    let plaintext = vec![0x31; 98_321];
    let stored = stored_crypto::encrypt(&plaintext).await.unwrap();
    assert_ne!(stored, plaintext);
    assert_eq!(stored_crypto::decrypt(&stored).await.unwrap(), plaintext);
    let root = car::raw_cid(&stored, 0x55);
    let other = car::raw_cid(b"different stored DAG", 0x55);
    for protocol in [ClusterProtocol::Proxy, ClusterProtocol::Rest] {
        for stream in [true, false] {
            for mismatch in [false, true] {
                let source = MockServer::start().await;
                let target = MockServer::start().await;
                Mock::given(method("POST"))
                    .and(path("/api/v0/files/stat"))
                    .respond_with(
                        ResponseTemplate::new(200)
                            .set_body_json(json!({"Hash":root, "Type":"file"})),
                    )
                    .expect(1)
                    .mount(&source)
                    .await;
                Mock::given(method("POST"))
                    .and(path("/api/v0/cat"))
                    .respond_with(ResponseTemplate::new(200).set_body_bytes(stored.clone()))
                    .expect(1)
                    .mount(&source)
                    .await;
                add(
                    &target,
                    protocol,
                    if mismatch { &other } else { &root },
                    stream,
                )
                .await;
                let out = ClusterProvider::new(options(
                    &source.uri(),
                    &target.uri(),
                    protocol,
                    ClusterStrategy::Upload,
                    stream,
                ))
                .unwrap()
                .submit_observed(request(&root))
                .await;
                if mismatch {
                    assert_eq!(
                        out.result.unwrap_err().code,
                        ClusterFailureCode::CidMismatch
                    );
                    assert_eq!(out.effect, ClusterWriteEffect::Unknown);
                    assert!(out.resources.iter().any(|r| r.resource.cid == other
                        && r.status == ClusterResourceStatus::PinAccepted));
                    assert!(out.replicas.is_none());
                } else {
                    assert!(out.result.is_ok(), "{out:?}");
                    assert_eq!(out.effect, ClusterWriteEffect::Observed);
                    assert_eq!(out.replication_outcome(), ReplicaOutcome::Unknown); // no fake N/N endpoint
                }
                assert!(
                    out.resources
                        .iter()
                        .all(|r| r.ownership == Ownership::Unknown)
                );
                let all = target.received_requests().await.unwrap();
                let posts: Vec<_> = all.iter().filter(|r| r.method == "POST").collect();
                assert_eq!(posts.len(), 1, "no repost/cid/CAR fallback");
                assert!(
                    all.iter()
                        .all(|r| r.method != "DELETE" && !r.url.path().contains("pin/rm"))
                );
                assert!(
                    posts[0]
                        .url
                        .query_pairs()
                        .any(|(k, v)| k == "format" && v == "unixfs")
                );
                assert!(posts[0].body.windows(stored.len()).any(|b| b == stored));
                assert!(
                    !posts[0]
                        .body
                        .windows(plaintext.len())
                        .any(|b| b == plaintext)
                );
                let reads = source.received_requests().await.unwrap();
                assert_eq!(reads.len(), 2);
                assert!(reads.iter().all(|r| r.url.path() != "/api/v0/dag/export"));
            }
        }
    }
}

#[tokio::test]
async fn cluster_provider_real_contract_cid_failure_never_reads_source_or_uploads() {
    for protocol in [ClusterProtocol::Proxy, ClusterProtocol::Rest] {
        let source = MockServer::start().await;
        let target = MockServer::start().await;
        let root = car::raw_cid(b"source only", 0x55);
        let route = if protocol == ClusterProtocol::Proxy {
            "/api/v0/pin/add".into()
        } else {
            format!("/pins/{root}")
        };
        Mock::given(method("POST"))
            .and(path(route))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&target)
            .await;
        let out = ClusterProvider::new(options(
            &source.uri(),
            &target.uri(),
            protocol,
            ClusterStrategy::Cid,
            true,
        ))
        .unwrap()
        .submit_observed(request(&root))
        .await;
        assert!(out.result.is_err());
        assert_eq!(out.effect, ClusterWriteEffect::Unknown);
        assert!(
            out.resources
                .iter()
                .all(|r| r.ownership == Ownership::Unknown)
        );
        assert!(out.replicas.is_none());
        assert!(source.received_requests().await.unwrap().is_empty());
        assert_eq!(target.received_requests().await.unwrap().len(), 1);
    }
}
