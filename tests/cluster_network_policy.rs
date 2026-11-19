use ipfs_s3_gateway::{crypto, pinning};
use pinning::{
    cluster::{
        ClusterAuth, ClusterPinset,
        provider::{
            ClusterProtocol, ClusterProvider, ClusterProviderOptions, ClusterSource,
            ClusterStrategy,
        },
    },
    provider::SubmitPin,
};

#[path = "../src/pinning/cluster/mod.rs"]
#[allow(dead_code, unused_imports)]
mod cluster;
use std::{collections::BTreeMap, time::Duration};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

#[test]
fn public_dns_endpoints_are_accepted_without_constructor_network_io() {
    let options = ClusterProviderOptions {
        protocol: ClusterProtocol::Proxy,
        strategy: ClusterStrategy::Upload,
        rest_endpoint: "https://rest.cluster.example.com".into(),
        proxy_endpoint: Some("https://proxy.cluster.example.com".into()),
        source: Some(ClusterSource {
            endpoint: "https://source.cluster.example.com".into(),
            auth: None,
        }),
        pinset: ClusterPinset::new("cluster", "network-contract").unwrap(),
        allow_private_network: false,
        ..ClusterProviderOptions::default()
    };
    assert!(
        ClusterProvider::new(options).is_ok(),
        "public DNS names must not be rejected merely for not being IP literals"
    );
}

#[test]
fn provider_constructor_requires_opt_in_for_private_endpoints() {
    for endpoint in [
        "http://127.0.0.1:9094",
        "https://localhost:9094",
        "https://169.254.169.254",
    ] {
        assert!(
            ClusterProvider::new(ClusterProviderOptions {
                rest_endpoint: endpoint.into(),
                pinset: ClusterPinset::new("cluster", "scope").unwrap(),
                ..ClusterProviderOptions::default()
            })
            .is_err(),
            "default provider bypassed network policy: {endpoint}"
        );
    }
}

#[tokio::test]
async fn ambient_proxy_policy_is_verified_in_an_isolated_child_executable() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = format!("http://{}", listener.local_addr().unwrap());
    // No proxy service is available. A client consulting any ambient proxy
    // cannot reach the mocks. Only the child gets these environment overrides.
    drop(listener);
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap());
    child
        .args([
            "--exact",
            "network_proxy_child_entry",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("CLUSTER_NETWORK_CHILD", "proxy-contract")
        .env_remove("NO_PROXY")
        .env_remove("no_proxy")
        .kill_on_drop(true);
    for name in [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        child.env(name, &proxy);
    }
    let output = tokio::time::timeout(Duration::from_secs(20), child.output())
        .await
        .expect("child deadline")
        .unwrap();
    assert!(
        output.status.success(),
        "child failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
#[ignore = "only launched by the isolated proxy parent with explicit child opt-in"]
async fn network_proxy_child_entry() {
    assert_eq!(
        std::env::var("CLUSTER_NETWORK_CHILD").as_deref(),
        Ok("proxy-contract")
    );
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    let rest = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"Hash":CID,"Type":"file"})),
        )
        .expect(1)
        .mount(&source)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(Vec::new()))
        .expect(1)
        .mount(&source)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"Name":"content","Hash":CID}))
                .insert_header("X-Chunked-Output", "1"),
        )
        .expect(1)
        .mount(&target)
        .await;
    let options = ClusterProviderOptions {
        protocol: ClusterProtocol::Proxy,
        strategy: ClusterStrategy::Upload,
        rest_endpoint: rest.uri(),
        proxy_endpoint: Some(target.uri()),
        rest_auth: Some(ClusterAuth::Bearer("rest-only".into())),
        proxy_auth: Some(ClusterAuth::Bearer("proxy-only".into())),
        source: Some(ClusterSource {
            endpoint: source.uri(),
            auth: Some(ClusterAuth::Bearer("source-only".into())),
        }),
        pinset: ClusterPinset::new("cluster", "network-contract").unwrap(),
        allow_private_network: true,
        ..ClusterProviderOptions::default()
    };
    let request = SubmitPin {
        cid: CID.into(),
        name: "opaque".into(),
        metadata: BTreeMap::new(),
    };
    let proxy = ClusterProvider::new(options.clone()).unwrap();
    assert_eq!(proxy.submit(request.clone()).await.unwrap().request_id, CID);
    let writes = target.received_requests().await.unwrap();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].headers["authorization"], "Bearer proxy-only");
    assert!(rest.received_requests().await.unwrap().is_empty());

    Mock::given(method("POST"))
        .and(path(format!("/pins/{CID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"cid":CID})))
        .expect(1)
        .mount(&rest)
        .await;
    let native = ClusterProvider::new(ClusterProviderOptions {
        protocol: ClusterProtocol::Rest,
        strategy: ClusterStrategy::Cid,
        proxy_endpoint: None,
        proxy_auth: None,
        source: None,
        ..options
    })
    .unwrap();
    assert_eq!(native.submit(request).await.unwrap().request_id, CID);
    let writes = rest.received_requests().await.unwrap();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].headers["authorization"], "Bearer rest-only");
    assert!(
        source
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| r.headers["authorization"] == "Bearer source-only")
    );
    // Exercise HTTPS as well under the isolated HTTPS_PROXY/ALL_PROXY settings.
    // This reuses the actual owned-TLS contract; no user CA/system state changes.
    cluster::network_tls_tests::tls_origin_contract().await;
}
