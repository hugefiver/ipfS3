use ipfs_s3_gateway::pinning;

#[path = "../src/pinning/cluster/mod.rs"]
#[allow(dead_code, unused_imports)] // Standalone leaf wiring, not shared registration.
mod cluster;

use cluster::provider::{
    ClusterFailureCode, ClusterProtocol, ClusterProvider, ClusterProviderOptions, ClusterRemoteRef,
    ClusterResourceStatus, ClusterSource, ClusterStrategy, ClusterWriteEffect,
};
use cluster::{ClusterAuth, ClusterPinset, ReplicaOutcome};
use pinning::{identity::Ownership, provider::SubmitPin};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const PEER: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";

fn options(
    server: &MockServer,
    protocol: ClusterProtocol,
    strategy: ClusterStrategy,
) -> ClusterProviderOptions {
    ClusterProviderOptions {
        protocol,
        strategy,
        rest_endpoint: server.uri(),
        proxy_endpoint: (protocol == ClusterProtocol::Proxy).then(|| server.uri()),
        pinset: ClusterPinset::new("cluster-contract", "pinset").unwrap(),
        min_replicas: 1,
        allow_private_network: true,
        ..ClusterProviderOptions::default()
    }
}

fn request(cid: &str) -> SubmitPin {
    SubmitPin {
        cid: cid.into(),
        name: "private-object-name".into(),
        metadata: BTreeMap::from([("secret".into(), "do-not-send".into())]),
    }
}

async fn replicas(server: &MockServer, cid: &str, state: &str) {
    let created = (chrono::Utc::now() - chrono::Duration::seconds(60)).to_rfc3339();
    let observed = (chrono::Utc::now() - chrono::Duration::seconds(1)).to_rfc3339();
    Mock::given(method("GET"))
        .and(path("/version"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"version":"1.1.6"})),
        )
        .mount(server)
        .await;
    Mock::given(method("GET")).and(path(format!("/allocations/{cid}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"cid":cid,"allocations":[PEER],"timestamp":created,"replication_factor_min":1,"replication_factor_max":1})))
        .mount(server).await;
    Mock::given(method("GET")).and(path(format!("/pins/{cid}"))).and(query_param("local", "false"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"cid":cid,"allocations":[PEER],"created":created,"peer_map":{PEER:{"status":state,"timestamp":observed,"error":""}}})))
        .mount(server).await;
}

#[tokio::test]
async fn cid_protocols_use_one_write_endpoint_then_fresh_rest_proof() {
    for protocol in [ClusterProtocol::Proxy, ClusterProtocol::Rest] {
        let server = MockServer::start().await;
        let route = if protocol == ClusterProtocol::Proxy {
            "/api/v0/pin/add".into()
        } else {
            format!("/pins/{CID}")
        };
        let body = if protocol == ClusterProtocol::Proxy {
            serde_json::json!({"Pins":[CID]})
        } else {
            serde_json::json!({"cid":CID,"type":"pin","allocations":[PEER]})
        };
        Mock::given(method("POST"))
            .and(path(route))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1)
            .mount(&server)
            .await;
        replicas(&server, CID, "pinned").await;
        let provider =
            ClusterProvider::new(options(&server, protocol, ClusterStrategy::Cid)).unwrap();
        let result = provider.submit_observed(request(CID)).await;
        assert!(result.result.is_ok(), "{result:?}");
        assert_eq!(result.effect, ClusterWriteEffect::Observed);
        assert_eq!(result.replication_outcome(), ReplicaOutcome::Complete);
        assert!(
            result
                .resources
                .iter()
                .all(|r| r.ownership == Ownership::Unknown)
        );
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.iter().filter(|r| r.method == "POST").count(), 1);
        assert!(!format!("{requests:?}").contains("private-object-name"));
    }
}

#[tokio::test]
async fn forbidden_after_dispatch_retains_typed_unknown_footprint_without_repost() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(403).set_body_string("secret upstream body"))
        .expect(1)
        .mount(&server)
        .await;
    let provider = ClusterProvider::new(options(
        &server,
        ClusterProtocol::Proxy,
        ClusterStrategy::Cid,
    ))
    .unwrap();
    let result = provider.submit_observed(request(CID)).await;
    assert!(result.result.is_err());
    assert_eq!(result.effect, ClusterWriteEffect::Unknown);
    assert_eq!(result.resources.len(), 1);
    assert_eq!(result.resources[0].resource.cid, CID);
    assert_eq!(result.result.unwrap_err().http_status, Some(403));
}

#[tokio::test]
async fn invalid_endpoint_or_missing_rest_never_constructs_proxy() {
    let server = MockServer::start().await;
    for bad in [
        "",
        "http://user:password@localhost:9000",
        "http://localhost:9000?token=secret",
    ] {
        let mut opts = options(&server, ClusterProtocol::Proxy, ClusterStrategy::Cid);
        opts.rest_endpoint = bad.into();
        assert!(ClusterProvider::new(opts).is_err());
    }
    let mut opts = options(&server, ClusterProtocol::Proxy, ClusterStrategy::Cid);
    opts.allow_private_network = false;
    assert!(ClusterProvider::new(opts).is_err());
}

fn raw_cid(payload: &[u8], codec: u64) -> String {
    let hash = cid::multihash::Multihash::wrap(0x12, &Sha256::digest(payload)).unwrap();
    cid::Cid::new_v1(codec, hash).to_string()
}

fn varint(mut value: usize) -> Vec<u8> {
    let mut out = Vec::new();
    while value >= 128 {
        out.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    out.push(value as u8);
    out
}
fn car_bytes(roots: &[String], blocks: &[(String, Vec<u8>)]) -> Vec<u8> {
    let mut header = vec![0xa2, 0x65];
    header.extend(b"roots");
    header.push(0x80 + roots.len() as u8);
    for root in roots {
        let encoded = cid::Cid::try_from(root.as_str()).unwrap().to_bytes();
        header.extend([0xd8, 42, 0x58, (encoded.len() + 1) as u8, 0]);
        header.extend(encoded);
    }
    header.push(0x67);
    header.extend(b"version");
    header.push(1);
    let mut out = varint(header.len());
    out.extend(header);
    for (cid, data) in blocks {
        let encoded = cid::Cid::try_from(cid.as_str()).unwrap().to_bytes();
        out.extend(varint(encoded.len() + data.len()));
        out.extend(encoded);
        out.extend(data);
    }
    out
}

fn directory(child: &str) -> (String, Vec<u8>) {
    let cid = cid::Cid::try_from(child).unwrap().to_bytes();
    let mut link = vec![10];
    link.extend(varint(cid.len()));
    link.extend(cid);
    link.extend([18, 5]);
    link.extend(b"entry");
    link.extend([24, 0]);
    let mut block = vec![10, 2, 8, 1, 18];
    block.extend(varint(link.len()));
    block.extend(link);
    (raw_cid(&block, 0x70), block)
}

fn added(protocol: ClusterProtocol, cid: &str) -> serde_json::Value {
    // Official v1.1.6 wire DTOs, not a fixture-side translation.
    match protocol {
        ClusterProtocol::Proxy => {
            serde_json::json!({"Name":"content", "Hash":cid,"Bytes":5,"Size":"5"})
        }
        ClusterProtocol::Rest => {
            serde_json::json!({"name":"content", "cid":cid,"size":5,"allocations":[PEER]})
        }
    }
}

async fn mount_add(
    server: &MockServer,
    protocol: ClusterProtocol,
    cid: &str,
    car: bool,
    stream: bool,
) {
    let route = if protocol == ClusterProtocol::Proxy {
        "/api/v0/add"
    } else {
        "/add"
    };
    let mut response = ResponseTemplate::new(200).insert_header("Content-Type", "application/json");
    if stream {
        response = response
            .insert_header("X-Chunked-Output", "1")
            .set_body_raw(format!("{}\n", added(protocol, cid)), "application/json");
    } else {
        response = response.set_body_json(serde_json::json!([added(protocol, cid)]));
    }
    Mock::given(method("POST"))
        .and(path(route))
        .and(query_param("format", if car { "car" } else { "unixfs" }))
        .and(query_param(
            "stream-channels",
            if stream { "true" } else { "false" },
        ))
        .and(query_param("no-pin", "false"))
        .and(query_param("wrap-with-directory", "false"))
        .respond_with(response.insert_header("Content-Type", "application/json"))
        .mount(server)
        .await;
}

async fn mount_export(source: &MockServer, cid: &str, bytes: Vec<u8>) {
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/export"))
        .and(query_param("arg", cid))
        .and(query_param("offline", "false"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(bytes))
        .expect(1)
        .mount(source)
        .await;
}

#[tokio::test]
async fn proxy_pascalcase_and_rest_lowercase_add_dtos_are_independent_in_both_modes() {
    for protocol in [ClusterProtocol::Proxy, ClusterProtocol::Rest] {
        for stream in [false, true] {
            let source = MockServer::start().await;
            let target = MockServer::start().await;
            mount_export(
                &source,
                CID,
                car_bytes(&[CID.into()], &[(CID.into(), vec![])]),
            )
            .await;
            let progress = match protocol {
                ClusterProtocol::Proxy => serde_json::json!({"Name":"content", "Bytes":0}),
                ClusterProtocol::Rest => serde_json::json!({"name":"content", "cid":null}),
            };
            // Size/Bytes are omitempty on the official zero-byte response.
            let root = match protocol {
                ClusterProtocol::Proxy => serde_json::json!({"Name":"content", "Hash":CID}),
                ClusterProtocol::Rest => serde_json::json!({"name":"content", "cid":CID}),
            };
            let body = if stream {
                format!("{progress}\n{root}\n")
            } else {
                serde_json::json!([progress, root]).to_string()
            };
            let route = if protocol == ClusterProtocol::Proxy {
                "/api/v0/add"
            } else {
                "/add"
            };
            Mock::given(method("POST"))
                .and(path(route))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_raw(body, "application/json")
                        .insert_header("X-Chunked-Output", "1"),
                )
                .expect(1)
                .mount(&target)
                .await;
            replicas(&target, CID, "pinned").await;
            let mut opts = options(&target, protocol, ClusterStrategy::Car);
            opts.stream_channels = stream;
            opts.source = Some(ClusterSource {
                endpoint: source.uri(),
                auth: None,
            });
            let out = ClusterProvider::new(opts)
                .unwrap()
                .submit_observed(request(CID))
                .await;
            assert!(out.result.is_ok(), "{protocol:?} stream={stream} {out:?}");
            assert_eq!(out.replication_outcome(), ReplicaOutcome::Complete);
        }
    }
}

#[tokio::test]
async fn wrong_protocol_and_wrong_size_types_fail_without_translation_or_fallback() {
    for protocol in [ClusterProtocol::Proxy, ClusterProtocol::Rest] {
        let other = if protocol == ClusterProtocol::Proxy {
            ClusterProtocol::Rest
        } else {
            ClusterProtocol::Proxy
        };
        let mut bad_size = added(protocol, CID);
        match protocol {
            ClusterProtocol::Proxy => bad_size["Size"] = serde_json::json!(5),
            ClusterProtocol::Rest => bad_size["size"] = serde_json::json!("5"),
        }
        for record in [added(other, CID), bad_size] {
            let source = MockServer::start().await;
            let target = MockServer::start().await;
            mount_export(
                &source,
                CID,
                car_bytes(&[CID.into()], &[(CID.into(), vec![])]),
            )
            .await;
            let route = if protocol == ClusterProtocol::Proxy {
                "/api/v0/add"
            } else {
                "/add"
            };
            Mock::given(method("POST"))
                .and(path(route))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_raw(format!("{record}\n"), "application/json")
                        .insert_header("X-Chunked-Output", "1"),
                )
                .expect(1)
                .mount(&target)
                .await;
            let mut opts = options(&target, protocol, ClusterStrategy::Car);
            opts.source = Some(ClusterSource {
                endpoint: source.uri(),
                auth: None,
            });
            let out = ClusterProvider::new(opts)
                .unwrap()
                .submit_observed(request(CID))
                .await;
            assert_eq!(out.result.unwrap_err().code, ClusterFailureCode::Protocol);
            assert_eq!(out.effect, ClusterWriteEffect::Unknown);
            assert!(out.replicas.is_none());
            assert_eq!(target.received_requests().await.unwrap().len(), 1);
        }
    }
}

#[tokio::test]
async fn stored_ciphertext_upload_is_opaque_and_auth_is_separate_in_both_response_modes() {
    use ipfs_s3_gateway::crypto::{
        ObjectKey,
        aes_gcm::{decrypt_chunk, encrypt_chunk},
    };
    let plaintext = b"private SSE plaintext must never leave the gateway";
    let key = ObjectKey { bytes: [0x37; 32] };
    let ciphertext = encrypt_chunk(&key, &[0x83; 12], plaintext).unwrap();
    assert_eq!(
        decrypt_chunk(&key, &ciphertext).unwrap().as_ref(),
        plaintext
    );
    for (protocol, stream) in [
        (ClusterProtocol::Proxy, true),
        (ClusterProtocol::Rest, false),
    ] {
        let source = MockServer::start().await;
        let target = MockServer::start().await;
        let root = raw_cid(&ciphertext, 0x55);
        Mock::given(method("POST"))
            .and(path("/api/v0/files/stat"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"Hash":root, "Type":"file"})),
            )
            .expect(1)
            .mount(&source)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ciphertext.clone()))
            .expect(1)
            .mount(&source)
            .await;
        mount_add(&target, protocol, &root, false, stream).await;
        replicas(&target, &root, "pinned").await;
        let mut opts = options(&target, protocol, ClusterStrategy::Upload);
        opts.stream_channels = stream;
        opts.source = Some(ClusterSource {
            endpoint: source.uri(),
            auth: Some(ClusterAuth::Bearer("source-only".into())),
        });
        opts.rest_auth = Some(ClusterAuth::Bearer("rest-only".into()));
        if protocol == ClusterProtocol::Proxy {
            opts.proxy_auth = Some(ClusterAuth::Bearer("proxy-only".into()));
        }
        let result = ClusterProvider::new(opts)
            .unwrap()
            .submit_observed(request(&root))
            .await;
        assert!(result.result.is_ok(), "{result:?}");
        assert_eq!(result.replication_outcome(), ReplicaOutcome::Complete);
        assert!(result.result.unwrap().accepted_at <= chrono::Utc::now());
        let requests = target.received_requests().await.unwrap();
        let post = requests.iter().find(|r| r.method == "POST").unwrap();
        assert!(
            post.body
                .windows(ciphertext.len())
                .any(|b| b == ciphertext.as_ref())
        );
        assert!(!post.body.windows(plaintext.len()).any(|b| b == plaintext));
        let body = String::from_utf8_lossy(&post.body);
        assert!(!body.contains("private-object-name") && !body.contains("do-not-send"));
        assert_eq!(
            post.headers["authorization"],
            if protocol == ClusterProtocol::Proxy {
                "Bearer proxy-only"
            } else {
                "Bearer rest-only"
            }
        );
        for r in &requests {
            assert!(!r.url.query().unwrap_or("").contains("replication-"));
            if r.method == "GET" {
                assert_eq!(r.headers["authorization"], "Bearer rest-only");
            }
        }
        assert!(
            source
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|r| r.headers["authorization"] == "Bearer source-only")
        );
    }
}

#[tokio::test]
async fn directory_upload_is_rejected_before_cat_and_directory_car_has_complete_closure() {
    let child = raw_cid(b"child", 0x55);
    let (root, block) = directory(&child);
    for protocol in [ClusterProtocol::Proxy, ClusterProtocol::Rest] {
        let source = MockServer::start().await;
        let target = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/files/stat"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"Hash":root,"Type":"directory"})),
            )
            .expect(1)
            .mount(&source)
            .await;
        let mut opts = options(&target, protocol, ClusterStrategy::Upload);
        opts.source = Some(ClusterSource {
            endpoint: source.uri(),
            auth: None,
        });
        let result = ClusterProvider::new(opts.clone())
            .unwrap()
            .submit_observed(request(&root))
            .await;
        assert_eq!(
            result.result.unwrap_err().code,
            ClusterFailureCode::DirectoryRequiresCar
        );
        assert_eq!(result.effect, ClusterWriteEffect::NotSubmitted);
        assert!(target.received_requests().await.unwrap().is_empty());
        opts.strategy = ClusterStrategy::Car;
        mount_export(
            &source,
            &root,
            car_bytes(
                std::slice::from_ref(&root),
                &[
                    (root.clone(), block.clone()),
                    (child.clone(), b"child".to_vec()),
                ],
            ),
        )
        .await;
        mount_add(&target, protocol, &root, true, true).await;
        replicas(&target, &root, "pinned").await;
        let result = ClusterProvider::new(opts)
            .unwrap()
            .submit_observed(request(&root))
            .await;
        assert!(result.result.is_ok(), "{result:?}");
        assert_eq!(result.replication_outcome(), ReplicaOutcome::Complete);
        assert!(
            source
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|r| r.url.path() != "/api/v0/cat")
        );
        assert!(
            target
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|r| r.url.path() != "/api/v0/dag/import")
        );
    }
}

#[tokio::test]
async fn incomplete_multiroot_wrongroot_duplicate_corrupt_and_trailing_cars_cannot_complete() {
    let child = raw_cid(b"child", 0x55);
    let (root, block) = directory(&child);
    let blocks = [
        (root.clone(), block.clone()),
        (child.clone(), b"child".to_vec()),
    ];
    let valid = car_bytes(std::slice::from_ref(&root), &blocks);
    let mut truncated = valid.clone();
    truncated.pop();
    let mut trailer = valid.clone();
    trailer.push(0x80);
    let mut corrupt = valid.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    let mut duplicate = blocks.to_vec();
    duplicate.push(blocks[0].clone());
    let bad_cars = [
        car_bytes(std::slice::from_ref(&root), &blocks[..1]),
        car_bytes(&[root.clone(), child.clone()], &blocks),
        car_bytes(std::slice::from_ref(&child), &blocks),
        car_bytes(std::slice::from_ref(&root), &duplicate),
        car_bytes(std::slice::from_ref(&root), &[]),
        truncated,
        trailer,
        corrupt,
        car_bytes(&[], &blocks),
        car_bytes(
            std::slice::from_ref(&root),
            &[
                blocks.to_vec(),
                vec![(raw_cid(b"unrelated", 0x55), b"unrelated".to_vec())],
            ]
            .concat(),
        ),
    ];
    for protocol in [ClusterProtocol::Proxy, ClusterProtocol::Rest] {
        for bytes in &bad_cars {
            let source = MockServer::start().await;
            let target = MockServer::start().await;
            mount_export(&source, &root, bytes.clone()).await;
            mount_add(&target, protocol, &root, true, true).await;
            let mut opts = options(&target, protocol, ClusterStrategy::Car);
            opts.source = Some(ClusterSource {
                endpoint: source.uri(),
                auth: None,
            });
            let result = ClusterProvider::new(opts)
                .unwrap()
                .submit_observed(request(&root))
                .await;
            assert_eq!(
                result.result.unwrap_err().code,
                ClusterFailureCode::InvalidCar
            );
            assert_eq!(result.effect, ClusterWriteEffect::Unknown);
            assert_eq!(result.resources[0].resource.cid, root);
            assert!(result.replicas.is_none());
            assert!(
                target
                    .received_requests()
                    .await
                    .unwrap()
                    .iter()
                    .all(|r| r.method != "GET")
            );
        }
    }
}

#[tokio::test]
async fn zero_and_small_stored_uploads_use_one_selected_protocol_and_preserve_cid() {
    for protocol in [ClusterProtocol::Proxy, ClusterProtocol::Rest] {
        for payload in [b"".as_slice(), b"small stored bytes".as_slice()] {
            let source = MockServer::start().await;
            let target = MockServer::start().await;
            let root = raw_cid(payload, 0x55);
            Mock::given(method("POST"))
                .and(path("/api/v0/files/stat"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!({"Hash":root,"Type":"raw"})),
                )
                .expect(1)
                .mount(&source)
                .await;
            Mock::given(method("POST"))
                .and(path("/api/v0/cat"))
                .respond_with(ResponseTemplate::new(200).set_body_bytes(payload))
                .expect(1)
                .mount(&source)
                .await;
            mount_add(&target, protocol, &root, false, true).await;
            replicas(&target, &root, "pinned").await;
            let mut opts = options(&target, protocol, ClusterStrategy::Upload);
            opts.source = Some(ClusterSource {
                endpoint: source.uri(),
                auth: None,
            });
            let out = ClusterProvider::new(opts)
                .unwrap()
                .submit_observed(request(&root))
                .await;
            assert!(out.result.is_ok(), "{protocol:?} {out:?}");
            assert_eq!(out.replication_outcome(), ReplicaOutcome::Complete);
            assert_eq!(out.resources.len(), 1);
            assert_eq!(out.resources[0].resource.cid, root);
            assert_eq!(source.received_requests().await.unwrap().len(), 2);
            assert_eq!(
                target
                    .received_requests()
                    .await
                    .unwrap()
                    .iter()
                    .filter(|r| r.method == "POST")
                    .count(),
                1
            );
        }
    }
}

#[tokio::test]
async fn upload_dag_mismatch_keeps_unexpected_pin_and_never_falls_back_to_car_or_other_protocol() {
    for protocol in [ClusterProtocol::Proxy, ClusterProtocol::Rest] {
        for stream in [false, true] {
            let source = MockServer::start().await;
            let target = MockServer::start().await;
            let root = raw_cid(b"original DAG", 0x70);
            let unexpected = raw_cid(b"different chunker DAG", 0x70);
            Mock::given(method("POST"))
                .and(path("/api/v0/files/stat"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!({"Hash":root,"Type":"file"})),
                )
                .expect(1)
                .mount(&source)
                .await;
            Mock::given(method("POST"))
                .and(path("/api/v0/cat"))
                .respond_with(ResponseTemplate::new(200).set_body_bytes(b"same stored file bytes"))
                .expect(1)
                .mount(&source)
                .await;
            mount_add(&target, protocol, &unexpected, false, stream).await;
            let mut opts = options(&target, protocol, ClusterStrategy::Upload);
            opts.stream_channels = stream;
            opts.unixfs = Some(cluster::provider::ClusterUnixfsOptions {
                cid_version: 1,
                raw_leaves: true,
                chunk_size: 131072,
                trickle: true,
            });
            opts.source = Some(ClusterSource {
                endpoint: source.uri(),
                auth: None,
            });
            let out = ClusterProvider::new(opts)
                .unwrap()
                .submit_observed(request(&root))
                .await;
            assert_eq!(
                out.result.unwrap_err().code,
                ClusterFailureCode::CidMismatch
            );
            assert_eq!(out.effect, ClusterWriteEffect::Unknown);
            assert_eq!(out.resources.len(), 2);
            assert!(
                out.resources.iter().any(|r| r.resource.cid == unexpected
                    && r.status == ClusterResourceStatus::PinAccepted)
            );
            assert!(
                out.resources
                    .iter()
                    .all(|r| r.ownership == Ownership::Unknown)
            );
            assert!(out.replicas.is_none());
            assert_eq!(source.received_requests().await.unwrap().len(), 2); // stat + cat; never dag/export
            let requests = target.received_requests().await.unwrap();
            assert_eq!(requests.len(), 1); // no alternate protocol, GET or cleanup
            let query: BTreeMap<_, _> = requests[0].url.query_pairs().collect();
            assert_eq!(query["format"], "unixfs");
            assert_eq!(query["chunker"], "size-131072");
            assert_eq!(query["layout"], "trickle");
            assert!(!query.keys().any(|key| key.contains("replication")));
        }
    }
}

#[tokio::test]
async fn source_rejection_before_dispatch_has_no_cluster_write_or_ownership_footprint() {
    for strategy in [ClusterStrategy::Upload, ClusterStrategy::Car] {
        for protocol in [ClusterProtocol::Proxy, ClusterProtocol::Rest] {
            let source = MockServer::start().await;
            let target = MockServer::start().await;
            let route = if strategy == ClusterStrategy::Car {
                "/api/v0/dag/export"
            } else {
                "/api/v0/files/stat"
            };
            Mock::given(method("POST"))
                .and(path(route))
                .respond_with(
                    ResponseTemplate::new(403).set_body_string("do-not-log-source-secret"),
                )
                .expect(1)
                .mount(&source)
                .await;
            let mut opts = options(&target, protocol, strategy);
            opts.source = Some(ClusterSource {
                endpoint: source.uri(),
                auth: None,
            });
            let out = ClusterProvider::new(opts)
                .unwrap()
                .submit_observed(request(CID))
                .await;
            assert_eq!(out.result.unwrap_err().http_status, Some(403));
            assert_eq!(out.effect, ClusterWriteEffect::NotSubmitted);
            assert!(out.resources.is_empty() && out.replicas.is_none());
            assert!(target.received_requests().await.unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn complete_single_root_car_copies_real_aes_gcm_stored_bytes_without_plaintext_or_cat() {
    use ipfs_s3_gateway::crypto::{ObjectKey, aes_gcm::encrypt_chunk};
    let plaintext = b"CAR private plaintext is never an export source";
    let ciphertext =
        encrypt_chunk(&ObjectKey { bytes: [0x51; 32] }, &[0x35; 12], plaintext).unwrap();
    let root = raw_cid(&ciphertext, 0x55);
    let car = car_bytes(
        std::slice::from_ref(&root),
        &[(root.clone(), ciphertext.to_vec())],
    );
    for protocol in [ClusterProtocol::Proxy, ClusterProtocol::Rest] {
        let source = MockServer::start().await;
        let target = MockServer::start().await;
        mount_export(&source, &root, car.clone()).await;
        mount_add(&target, protocol, &root, true, true).await;
        replicas(&target, &root, "pinned").await;
        let mut opts = options(&target, protocol, ClusterStrategy::Car);
        opts.source = Some(ClusterSource {
            endpoint: source.uri(),
            auth: None,
        });
        let out = ClusterProvider::new(opts)
            .unwrap()
            .submit_observed(request(&root))
            .await;
        assert!(out.result.is_ok(), "{out:?}");
        assert_eq!(out.replication_outcome(), ReplicaOutcome::Complete);
        let requests = target.received_requests().await.unwrap();
        let post = requests.iter().find(|r| r.method == "POST").unwrap();
        assert!(post.body.windows(car.len()).any(|bytes| bytes == car));
        assert!(
            !post
                .body
                .windows(plaintext.len())
                .any(|bytes| bytes == plaintext)
        );
        assert_eq!(source.received_requests().await.unwrap().len(), 1); // export only; no cat/decrypt
    }
}

#[tokio::test]
async fn cid_alias_is_equivalent_but_codec_mismatch_preserves_both_resources() {
    let dagpb = raw_cid(b"block", 0x70);
    let cid = cid::Cid::try_from(dagpb.as_str()).unwrap();
    let alias = cid::Cid::new_v0(*cid.hash()).unwrap().to_string();
    for returned in [alias, cid::Cid::new_v1(0x55, *cid.hash()).to_string()] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins":[returned]})),
            )
            .expect(1)
            .mount(&server)
            .await;
        replicas(&server, &dagpb, "pinned").await;
        let provider = ClusterProvider::new(options(
            &server,
            ClusterProtocol::Proxy,
            ClusterStrategy::Cid,
        ))
        .unwrap();
        let out = provider.submit_observed(request(&dagpb)).await;
        if returned.starts_with("Qm") {
            assert!(out.result.is_ok());
            assert_eq!(out.resources.len(), 1);
        } else {
            assert_eq!(
                out.result.unwrap_err().code,
                ClusterFailureCode::CidMismatch
            );
            assert_eq!(out.effect, ClusterWriteEffect::Unknown);
            assert_eq!(out.resources.len(), 2);
            assert!(
                out.resources.iter().any(|r| r.resource.cid == returned
                    && r.status == ClusterResourceStatus::PinAccepted)
            );
        }
    }
}

#[tokio::test]
async fn insufficient_or_unavailable_verifier_never_turns_write_receipt_into_complete() {
    for state in ["remote", "pinned"] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(format!("/pins/{CID}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"cid":CID})))
            .expect(1)
            .mount(&server)
            .await;
        replicas(&server, CID, state).await;
        let mut opts = options(&server, ClusterProtocol::Rest, ClusterStrategy::Cid);
        opts.min_replicas = 2;
        let provider = ClusterProvider::new(opts).unwrap();
        let out = provider.submit_observed(request(CID)).await;
        assert!(out.result.is_ok());
        assert_eq!(out.replication_outcome(), ReplicaOutcome::Insufficient);
        assert_eq!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .filter(|r| r.method == "POST")
                .count(),
            1
        );
        server.reset().await;
        let unknown = provider.observe(CID).await;
        assert_eq!(unknown.outcome(), ReplicaOutcome::Unknown);
        assert_eq!(out.replication_outcome(), ReplicaOutcome::Unknown); // Superseded, historical evidence retained.
        assert!(out.replicas.unwrap().evidence().is_some());
    }
}

#[tokio::test]
async fn redirected_and_timed_out_writes_are_unknown_and_never_fallback_or_repost() {
    for status in [302, 200] {
        let server = MockServer::start().await;
        let response = if status == 302 {
            ResponseTemplate::new(status).insert_header("Location", "/other-write")
        } else {
            ResponseTemplate::new(status)
                .set_delay(Duration::from_millis(200))
                .set_body_json(serde_json::json!({"cid":CID}))
        };
        Mock::given(method("POST"))
            .and(path(format!("/pins/{CID}")))
            .respond_with(response)
            .expect(1)
            .mount(&server)
            .await;
        let mut opts = options(&server, ClusterProtocol::Rest, ClusterStrategy::Cid);
        opts.transfer_limits.connect_timeout = Duration::from_millis(10);
        opts.transfer_limits.control_timeout = Duration::from_millis(40);
        opts.transfer_limits.idle_timeout = Duration::from_millis(20);
        let out = ClusterProvider::new(opts)
            .unwrap()
            .submit_observed(request(CID))
            .await;
        assert!(out.result.is_err());
        assert_eq!(out.effect, ClusterWriteEffect::Unknown);
        assert_eq!(out.resources.len(), 1);
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn typed_unpin_primitive_rejects_wrong_scope_and_never_runs_during_submit() {
    let server = MockServer::start().await;
    let provider = ClusterProvider::new(options(
        &server,
        ClusterProtocol::Rest,
        ClusterStrategy::Cid,
    ))
    .unwrap();
    let mut reference = ClusterRemoteRef {
        resource: provider.resource_key(CID).unwrap(),
        protocol: ClusterProtocol::Rest,
        strategy: ClusterStrategy::Cid,
    };
    reference.resource.pinset.scope = "other-scope".into();
    let out = provider
        .unpin_authorized_ref(&reference, &CancellationToken::new())
        .await;
    assert_eq!(
        out.result.unwrap_err().code,
        ClusterFailureCode::InvalidReference
    );
    assert_eq!(out.effect, ClusterWriteEffect::NotSubmitted);
    assert!(server.received_requests().await.unwrap().is_empty());
    // No valid deletion is executed: ledger authorization is outside this leaf.
}

mod loopback {
    use super::*;
    use axum::{
        Router,
        body::Body,
        extract::{Request, State},
        response::Response,
    };
    use bytes::Bytes;
    use http_body_util::{BodyExt as _, StreamBody};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    #[derive(Clone, Copy, PartialEq, Eq)]
    pub enum Mode {
        CleanSlow,
        SourceTrailer,
        TargetTrailer,
        SourceDisconnect,
        SourceStall,
        EarlyTarget,
        BlockTarget,
    }

    #[derive(Clone)]
    struct SourceState {
        bytes: Arc<Vec<u8>>,
        mode: Mode,
        produced: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
    }
    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    pub struct Server {
        pub endpoint: String,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    async fn serve(router: Router) -> Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Server { endpoint, task }
    }
    pub struct Source {
        pub server: Server,
        pub produced: Arc<AtomicUsize>,
        pub dropped: Arc<AtomicBool>,
    }
    pub async fn source(bytes: Vec<u8>, mode: Mode, root: &str) -> Source {
        let state = SourceState {
            bytes: Arc::new(bytes),
            mode,
            produced: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicBool::new(false)),
        };
        let produced = state.produced.clone();
        let dropped = state.dropped.clone();
        let stat = serde_json::json!({"Hash":root,"Type":"file"});
        let router = Router::new()
            .route(
                "/api/v0/files/stat",
                axum::routing::post(move || {
                    let stat = stat.clone();
                    async move { axum::Json(stat) }
                }),
            )
            .fallback(source_handler)
            .with_state(state);
        Source {
            server: serve(router).await,
            produced,
            dropped,
        }
    }
    async fn source_handler(State(state): State<SourceState>, request: Request) -> Response {
        assert_eq!(request.headers().get("te").unwrap(), "trailers");
        let stream = async_stream::stream! {
            let _drop = DropFlag(state.dropped.clone());
            for (i, chunk) in state.bytes.chunks(4096).enumerate() {
                if state.mode == Mode::CleanSlow { tokio::time::sleep(Duration::from_millis(15)).await; }
                if i == 1 && state.mode == Mode::SourceStall { std::future::pending::<()>().await; }
                if i == 1 && state.mode == Mode::SourceDisconnect {
                    yield Err::<hyper::body::Frame<Bytes>, std::io::Error>(std::io::Error::other("fixture source disconnected")); return;
                }
                state.produced.fetch_add(chunk.len(), Ordering::AcqRel);
                yield Ok(hyper::body::Frame::data(Bytes::copy_from_slice(chunk)));
            }
            if state.mode == Mode::SourceTrailer {
                let mut trailers = http::HeaderMap::new(); trailers.insert("x-stream-error", http::HeaderValue::from_static("source failure"));
                yield Ok(hyper::body::Frame::trailers(trailers));
            }
        };
        Response::builder()
            .status(200)
            .header("trailer", "X-Stream-Error")
            .body(Body::new(StreamBody::new(stream)))
            .unwrap()
    }
    pub async fn target(root: &str, mode: Mode) -> Server {
        let root = root.to_owned();
        serve(Router::new().fallback(move |mut request: Request| {
            let root = root.clone();
            async move {
                if request.method() != http::Method::POST { return Response::builder().status(404).body(Body::empty()).unwrap(); }
                if mode == Mode::BlockTarget { std::future::pending::<()>().await; }
                if mode != Mode::EarlyTarget {
                    while let Some(frame) = request.body_mut().frame().await {
                        if frame.is_err() { return Response::builder().status(400).body(Body::empty()).unwrap(); }
                    }
                }
                let output = Bytes::from(format!("{}\n", added(ClusterProtocol::Rest, &root)));
                let stream = async_stream::stream! {
                    yield Ok::<_, std::io::Error>(hyper::body::Frame::data(output));
                    if mode == Mode::TargetTrailer {
                        let mut trailers = http::HeaderMap::new(); trailers.insert("x-stream-error", http::HeaderValue::from_static("target finalize failure"));
                        yield Ok(hyper::body::Frame::trailers(trailers));
                    }
                };
                Response::builder().status(200).header("content-type", "application/json").header("x-chunked-output", "1")
                    .header("trailer", "X-Stream-Error").body(Body::new(StreamBody::new(stream))).unwrap()
            }
        })).await
    }
    pub async fn early_target(root: &str) -> Server {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let output = format!("{}\n", added(ClusterProtocol::Rest, root));
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Chunked-Output: 1\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{output}",
            output.len()
        );
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut headers = Vec::new();
            let mut chunk = [0; 1024];
            while !headers.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0 && headers.len() < 64 * 1024);
                headers.extend_from_slice(&chunk[..n]);
            }
            socket.write_all(response.as_bytes()).await.unwrap();
            // Keep TCP open without consuming the upload: no Windows reset can
            // hide the valid early receipt, and source cannot run to EOF.
            std::future::pending::<()>().await;
        });
        Server { endpoint, task }
    }
    pub fn opts(
        source: &Source,
        target: &Server,
        strategy: ClusterStrategy,
    ) -> ClusterProviderOptions {
        ClusterProviderOptions {
            protocol: ClusterProtocol::Rest,
            strategy,
            rest_endpoint: target.endpoint.clone(),
            source: Some(ClusterSource {
                endpoint: source.server.endpoint.clone(),
                auth: None,
            }),
            pinset: ClusterPinset::new("loopback", "one-pinset").unwrap(),
            allow_private_network: true,
            ..ClusterProviderOptions::default()
        }
    }
}

#[tokio::test]
async fn source_and_target_trailers_or_source_disconnect_cannot_fake_completion() {
    use loopback::Mode;
    let data = vec![0x81; 16 * 1024];
    let root = raw_cid(&data, 0x55);
    let car = car_bytes(std::slice::from_ref(&root), &[(root.clone(), data)]);
    for mode in [
        Mode::SourceTrailer,
        Mode::TargetTrailer,
        Mode::SourceDisconnect,
    ] {
        let source = loopback::source(car.clone(), mode, &root).await;
        let target = loopback::target(&root, mode).await;
        let out = ClusterProvider::new(loopback::opts(&source, &target, ClusterStrategy::Car))
            .unwrap()
            .submit_observed(request(&root))
            .await;
        assert!(out.result.is_err(), "mode={} {out:?}", mode as u8);
        if mode == Mode::SourceDisconnect && out.resources.is_empty() {
            assert_eq!(out.effect, ClusterWriteEffect::NotSubmitted);
        } else {
            assert_eq!(
                out.effect,
                ClusterWriteEffect::Unknown,
                "mode={} {out:?}",
                mode as u8
            );
        }
        assert_eq!(out.replication_outcome(), ReplicaOutcome::Unknown);
        assert!(out.replicas.is_none());
        if mode == Mode::TargetTrailer {
            assert_eq!(out.http_status, Some(200));
            assert!(
                out.resources
                    .iter()
                    .any(|r| r.status == ClusterResourceStatus::Reported)
            );
        }
    }
}

#[tokio::test]
async fn continuous_slow_source_progress_outlives_idle_window_without_absolute_timeout() {
    let data = vec![0x51; 32 * 1024];
    let root = raw_cid(&data, 0x55);
    let source = loopback::source(
        car_bytes(std::slice::from_ref(&root), &[(root.clone(), data)]),
        loopback::Mode::CleanSlow,
        &root,
    )
    .await;
    let target = loopback::target(&root, loopback::Mode::CleanSlow).await;
    let mut opts = loopback::opts(&source, &target, ClusterStrategy::Car);
    opts.transfer_limits.idle_timeout = Duration::from_millis(100);
    let start = tokio::time::Instant::now();
    let out = ClusterProvider::new(opts)
        .unwrap()
        .submit_observed(request(&root))
        .await;
    assert!(out.result.is_ok(), "{out:?}");
    assert!(start.elapsed() > Duration::from_millis(100));
    // Target REST is deliberately unavailable; acceptance is not N/N proof.
    assert_eq!(out.replication_outcome(), ReplicaOutcome::Unknown);
}

#[tokio::test]
async fn early_destination_response_does_not_replace_source_and_multipart_eof() {
    let data = vec![0x42; 32 * 1024 * 1024];
    let root = raw_cid(&data, 0x55);
    let source = loopback::source(data, loopback::Mode::EarlyTarget, &root).await;
    let target = loopback::early_target(&root).await;
    let out = ClusterProvider::new(loopback::opts(&source, &target, ClusterStrategy::Upload))
        .unwrap()
        .submit_observed(request(&root))
        .await;
    assert_eq!(
        out.result.unwrap_err().code,
        ClusterFailureCode::IncompleteUpload
    );
    assert_eq!(out.http_status, Some(200));
    assert_eq!(out.effect, ClusterWriteEffect::Unknown);
    assert!(
        out.resources
            .iter()
            .any(|r| r.status == ClusterResourceStatus::Reported)
    );
}

#[tokio::test]
async fn idle_timeout_and_explicit_cancel_drop_source_without_unbounded_prefetch() {
    use std::sync::atomic::Ordering;
    for cancel_write in [false, true] {
        let data = vec![0x42; 32 * 1024 * 1024];
        let root = raw_cid(&data, 0x55);
        let source = loopback::source(data, loopback::Mode::BlockTarget, &root).await;
        let target = loopback::target(&root, loopback::Mode::BlockTarget).await;
        let mut opts = loopback::opts(&source, &target, ClusterStrategy::Upload);
        opts.transfer_limits.idle_timeout = Duration::from_millis(200);
        let provider = ClusterProvider::new(opts).unwrap();
        let token = CancellationToken::new();
        let future = provider.submit_with_cancel(request(&root), &token);
        tokio::pin!(future);
        let out = if cancel_write {
            tokio::select! {
                result = &mut future => panic!("completed before cancellation: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(100)) => token.cancel(),
            }
            future.await
        } else {
            future.await
        };
        assert_eq!(
            out.result.unwrap_err().code,
            if cancel_write {
                ClusterFailureCode::Cancelled
            } else {
                ClusterFailureCode::IdleTimeout
            }
        );
        assert_eq!(out.effect, ClusterWriteEffect::Unknown);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !source.dropped.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("cancelled HTTP source stream must be released");
        assert!(
            source.produced.load(Ordering::Acquire) < 32 * 1024 * 1024,
            "source was eagerly collected despite a stalled destination"
        );
    }
}

#[tokio::test]
async fn source_stall_and_pre_dispatch_cancel_preserve_distinct_effect_certainty() {
    let data = vec![0x51; 16 * 1024];
    let root = raw_cid(&data, 0x55);
    let source = loopback::source(
        car_bytes(std::slice::from_ref(&root), &[(root.clone(), data)]),
        loopback::Mode::SourceStall,
        &root,
    )
    .await;
    let target = loopback::target(&root, loopback::Mode::SourceStall).await;
    let mut opts = loopback::opts(&source, &target, ClusterStrategy::Car);
    opts.transfer_limits.idle_timeout = Duration::from_millis(100);
    let provider = ClusterProvider::new(opts).unwrap();
    let token = CancellationToken::new();
    token.cancel();
    let not_sent = provider.submit_with_cancel(request(&root), &token).await;
    assert_eq!(not_sent.effect, ClusterWriteEffect::NotSubmitted);
    assert!(not_sent.resources.is_empty());
    let unknown = provider.submit_observed(request(&root)).await;
    assert_eq!(
        unknown.result.unwrap_err().code,
        ClusterFailureCode::IdleTimeout
    );
    assert_eq!(unknown.effect, ClusterWriteEffect::Unknown);
}

#[tokio::test]
async fn same_pinset_addresses_share_one_ledger_resource_and_proof_expiration_is_dynamic() {
    let one = MockServer::start().await;
    let two = MockServer::start().await;
    for server in [&one, &two] {
        Mock::given(method("POST"))
            .and(path(format!("/pins/{CID}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"cid":CID})))
            .expect(1)
            .mount(server)
            .await;
        replicas(server, CID, "pinned").await;
    }
    let mut first = options(&one, ClusterProtocol::Rest, ClusterStrategy::Cid);
    first.verifier_limits.max_observation_age = Duration::from_millis(20);
    let first = ClusterProvider::new(first)
        .unwrap()
        .submit_observed(request(CID))
        .await;
    let second = ClusterProvider::new(options(&two, ClusterProtocol::Rest, ClusterStrategy::Cid))
        .unwrap()
        .submit_observed(request(CID))
        .await;
    assert_eq!(
        first.resources[0].ledger_key(),
        second.resources[0].ledger_key()
    );
    assert_eq!(
        first.resources[0].ledger_key().resource_type,
        pinning::identity::RemoteResourceType::ClusterPin
    );
    assert_eq!(first.resources[0].ownership, Ownership::Unknown);
    assert_eq!(second.replication_outcome(), ReplicaOutcome::Complete);
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(first.replication_outcome(), ReplicaOutcome::Unknown);
    assert!(first.replicas.unwrap().evidence().is_some());
}

#[tokio::test]
async fn malformed_duplicate_wrong_content_type_and_forbidden_receipts_keep_actual_cids() {
    let source = MockServer::start().await;
    let data = b"stored bytes";
    let root = raw_cid(data, 0x55);
    let other = raw_cid(b"other", 0x55);
    // Reusable source fixture: this case deliberately performs four distinct jobs.
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/export"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(car_bytes(
            std::slice::from_ref(&root),
            &[(root.clone(), data.to_vec())],
        )))
        .expect(4)
        .mount(&source)
        .await;
    for (status, body, content_type) in [
        (
            200,
            format!("{{\"cid\":\"{other}\",\"name\":3}}\n"),
            "application/json",
        ),
        (
            200,
            format!(
                "{}\n{}\n",
                added(ClusterProtocol::Rest, &other),
                added(ClusterProtocol::Rest, &root)
            ),
            "application/json",
        ),
        (
            200,
            format!("{}\n", added(ClusterProtocol::Rest, &other)),
            "text/plain",
        ),
        (
            403,
            format!("{}\n", added(ClusterProtocol::Rest, &other)),
            "application/json",
        ),
    ] {
        let target = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/add"))
            .respond_with(
                ResponseTemplate::new(status)
                    .set_body_raw(body, content_type)
                    .insert_header("X-Chunked-Output", "1"),
            )
            .expect(1)
            .mount(&target)
            .await;
        let mut opts = options(&target, ClusterProtocol::Rest, ClusterStrategy::Car);
        opts.source = Some(ClusterSource {
            endpoint: source.uri(),
            auth: None,
        });
        let out = ClusterProvider::new(opts)
            .unwrap()
            .submit_observed(request(&root))
            .await;
        assert!(out.result.is_err());
        assert_eq!(out.effect, ClusterWriteEffect::Unknown);
        assert_eq!(out.http_status, Some(status));
        assert!(
            out.resources
                .iter()
                .any(|r| r.resource.cid == other && r.status == ClusterResourceStatus::Reported)
        );
        assert!(out.resources.iter().any(|r| r.resource.cid == root));
        assert!(out.replicas.is_none());
        assert_eq!(target.received_requests().await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn dag_cbor_links_and_sha512_block_hashes_are_validated_without_codec_substitution() {
    use sha2::Sha512;
    let child_data = b"ciphertext leaf".to_vec();
    let child_hash = cid::multihash::Multihash::wrap(0x13, &Sha512::digest(&child_data)).unwrap();
    let child = cid::Cid::new_v1(0x55, child_hash);
    let bytes = child.to_bytes();
    let mut cbor = vec![0xa1, 0x64];
    cbor.extend(b"link");
    cbor.extend([0xd8, 42, 0x58, (bytes.len() + 1) as u8, 0]);
    cbor.extend(bytes);
    let root = raw_cid(&cbor, 0x71);
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    mount_export(
        &source,
        &root,
        car_bytes(
            std::slice::from_ref(&root),
            &[(root.clone(), cbor), (child.to_string(), child_data)],
        ),
    )
    .await;
    mount_add(&target, ClusterProtocol::Rest, &root, true, true).await;
    replicas(&target, &root, "pinned").await;
    let mut opts = options(&target, ClusterProtocol::Rest, ClusterStrategy::Car);
    opts.source = Some(ClusterSource {
        endpoint: source.uri(),
        auth: None,
    });
    let out = ClusterProvider::new(opts)
        .unwrap()
        .submit_observed(request(&root))
        .await;
    assert!(out.result.is_ok(), "{out:?}");
    assert_eq!(out.replication_outcome(), ReplicaOutcome::Complete);
}

#[tokio::test]
async fn malformed_add_record_does_not_discard_later_reported_cids_in_stream_or_array() {
    let root = raw_cid(b"stored", 0x55);
    let first = raw_cid(b"first unexpected resource", 0x55);
    let later = raw_cid(b"later unexpected resource", 0x55);
    for protocol in [ClusterProtocol::Proxy, ClusterProtocol::Rest] {
        for stream in [false, true] {
            let source = MockServer::start().await;
            let target = MockServer::start().await;
            mount_export(
                &source,
                &root,
                car_bytes(
                    std::slice::from_ref(&root),
                    &[(root.clone(), b"stored".to_vec())],
                ),
            )
            .await;
            let mut malformed = added(protocol, &first);
            malformed[if protocol == ClusterProtocol::Proxy {
                "Name"
            } else {
                "name"
            }] = serde_json::json!(3);
            let valid = added(protocol, &later);
            let body = if stream {
                format!("{malformed}\n{valid}\n")
            } else {
                serde_json::json!([malformed, valid]).to_string()
            };
            let route = if protocol == ClusterProtocol::Proxy {
                "/api/v0/add"
            } else {
                "/add"
            };
            Mock::given(method("POST"))
                .and(path(route))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_raw(body, "application/json")
                        .insert_header("X-Chunked-Output", "1"),
                )
                .expect(1)
                .mount(&target)
                .await;
            let mut opts = options(&target, protocol, ClusterStrategy::Car);
            opts.stream_channels = stream;
            opts.source = Some(ClusterSource {
                endpoint: source.uri(),
                auth: None,
            });
            let out = ClusterProvider::new(opts)
                .unwrap()
                .submit_observed(request(&root))
                .await;
            assert_eq!(out.result.unwrap_err().code, ClusterFailureCode::Protocol);
            assert_eq!(out.effect, ClusterWriteEffect::Unknown);
            for cid in [&root, &first, &later] {
                assert!(
                    out.resources.iter().any(|r| &r.resource.cid == cid),
                    "lost {cid}: {protocol:?}, stream={stream}, {:?}",
                    out.resources
                );
            }
            assert!(
                out.resources
                    .iter()
                    .all(|r| r.ownership == Ownership::Unknown)
            );
            assert!(out.replicas.is_none());
            assert_eq!(target.received_requests().await.unwrap().len(), 1);
        }
    }
}

#[tokio::test]
async fn v0_unixfs_import_parameters_are_preserved_and_car_budgets_fail_closed() {
    let root = raw_cid(b"dagpb", 0x70);
    let parsed = cid::Cid::try_from(root.as_str()).unwrap();
    let v0 = cid::Cid::new_v0(*parsed.hash()).unwrap().to_string();
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"Hash":v0,"Type":"file"})),
        )
        .mount(&source)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"content"))
        .mount(&source)
        .await;
    mount_add(&target, ClusterProtocol::Proxy, &v0, false, true).await;
    replicas(&target, &root, "pinned").await;
    let mut opts = options(&target, ClusterProtocol::Proxy, ClusterStrategy::Upload);
    opts.source = Some(ClusterSource {
        endpoint: source.uri(),
        auth: None,
    });
    let out = ClusterProvider::new(opts)
        .unwrap()
        .submit_observed(request(&v0))
        .await;
    assert!(out.result.is_ok(), "{out:?}");
    let posts = target.received_requests().await.unwrap();
    let post = posts.iter().find(|r| r.method == "POST").unwrap();
    let params: BTreeMap<_, _> = post.url.query_pairs().collect();
    assert_eq!(params["cid-version"], "0");
    assert_eq!(params["raw-leaves"], "false");
    let data = vec![0x41; 4096];
    let root = raw_cid(&data, 0x55);
    let source = MockServer::start().await;
    mount_export(
        &source,
        &root,
        car_bytes(std::slice::from_ref(&root), &[(root.clone(), data)]),
    )
    .await;
    let mut opts = options(&target, ClusterProtocol::Proxy, ClusterStrategy::Car);
    opts.source = Some(ClusterSource {
        endpoint: source.uri(),
        auth: None,
    });
    opts.transfer_limits.max_block_bytes = 128;
    let out = ClusterProvider::new(opts)
        .unwrap()
        .submit_observed(request(&root))
        .await;
    assert!(out.result.is_err());
    assert_eq!(out.replication_outcome(), ReplicaOutcome::Unknown);
}

#[tokio::test]
async fn source_control_timeout_is_not_a_dispatched_cluster_mutation() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(200))
                .set_body_json(serde_json::json!({"Hash":CID,"Type":"file"})),
        )
        .expect(1)
        .mount(&source)
        .await;
    let mut opts = options(&target, ClusterProtocol::Rest, ClusterStrategy::Upload);
    opts.source = Some(ClusterSource {
        endpoint: source.uri(),
        auth: None,
    });
    opts.transfer_limits.connect_timeout = Duration::from_millis(10);
    opts.transfer_limits.control_timeout = Duration::from_millis(30);
    opts.transfer_limits.idle_timeout = Duration::from_millis(200);
    let out = ClusterProvider::new(opts)
        .unwrap()
        .submit_observed(request(CID))
        .await;
    assert_eq!(
        out.result.unwrap_err().code,
        ClusterFailureCode::ControlTimeout
    );
    assert_eq!(out.effect, ClusterWriteEffect::NotSubmitted);
    assert!(out.resources.is_empty());
    assert!(target.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn forbidden_missing_and_empty_rest_evidence_do_not_establish_absence_or_cleanup() {
    for status in [403, 404, 200] {
        let target = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(format!("/pins/{CID}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"cid":CID})))
            .expect(1)
            .mount(&target)
            .await;
        Mock::given(method("GET"))
            .and(path("/version"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"version":"1.1.6"})),
            )
            .mount(&target)
            .await;
        let created = (chrono::Utc::now() - chrono::Duration::seconds(60)).to_rfc3339();
        Mock::given(method("GET")).and(path(format!("/allocations/{CID}")))
            .respond_with(ResponseTemplate::new(status).set_body_json(serde_json::json!({"cid":CID,"allocations":[],"timestamp":created,"replication_factor_min":1,"replication_factor_max":1})))
            .mount(&target).await;
        Mock::given(method("GET"))
            .and(path(format!("/pins/{CID}")))
            .and(query_param("local", "false"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"cid":CID,"allocations":[],"created":created,"peer_map":{}}),
            ))
            .mount(&target)
            .await;
        let out = ClusterProvider::new(options(
            &target,
            ClusterProtocol::Rest,
            ClusterStrategy::Cid,
        ))
        .unwrap()
        .submit_observed(request(CID))
        .await;
        assert!(out.result.is_ok());
        assert!(matches!(
            out.replication_outcome(),
            ReplicaOutcome::Unknown | ReplicaOutcome::Insufficient
        ));
        assert_eq!(out.resources[0].ownership, Ownership::Unknown);
        assert!(out.replicas.is_some());
        let requests = target.received_requests().await.unwrap();
        assert_eq!(requests.iter().filter(|r| r.method == "POST").count(), 1);
        assert!(
            requests
                .iter()
                .all(|r| r.method != "DELETE" && !r.url.path().contains("pin/rm"))
        );
    }
}
