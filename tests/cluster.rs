#[path = "support/cluster.rs"]
mod cluster_support;
#[allow(dead_code)]
mod support;

use anyhow::{Result as AnyResult, anyhow, ensure};
use bytes::Bytes;
use cluster_support::{
    ClusterClient, RecoveryState, kubo_cat, peer_set_digest, wait_for_shared_two_peer_view,
    wait_for_two_pinned, wait_until_not_fully_pinned,
};
use futures_util::StreamExt;
use http::{HeaderMap, Method};
use ipfs_s3_gateway::kubo::{KuboClient, add::stream_add, cat::stream_cat, pin::pin_add};
use s3::{bucket::Bucket, bucket_ops::BucketConfiguration, creds::Credentials, region::Region};
use std::{
    future::Future,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use support::sigv4::send_sigv4;

const S3_TIMEOUT: Duration = Duration::from_secs(30);
const PROXY_CONTROL_TIMEOUT: Duration = Duration::from_secs(10);
const PROXY_DOWNLOAD_IDLE_TIMEOUT: Duration = Duration::from_secs(10);
const PROXY_SEQUENCE_TIMEOUT: Duration = Duration::from_secs(30);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(120);
static BUCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

fn endpoint(name: &str) -> String {
    let value = std::env::var(name).unwrap_or_else(|_| panic!("cluster_endpoint_required"));
    let value = value.trim_end_matches('/').to_owned();
    assert!(
        value.starts_with("http://127.0.0.1:"),
        "cluster_endpoint_not_loopback_http"
    );
    value
}

fn state_path() -> PathBuf {
    let path = PathBuf::from(
        std::env::var("IPFS_S3_CLUSTER_STATE_PATH")
            .unwrap_or_else(|_| panic!("cluster_state_path_required")),
    );
    assert!(path.is_absolute(), "cluster_state_path_not_absolute");
    path
}

fn credentials() -> Credentials {
    match Credentials::new(Some("test"), Some("test"), None, None, None) {
        Ok(credentials) => credentials,
        Err(_) => panic!("cluster_test_credentials_invalid"),
    }
}

fn region(endpoint: &str) -> Region {
    Region::Custom {
        region: "us-east-1".to_owned(),
        endpoint: endpoint.to_owned(),
    }
}

fn bucket_at(endpoint: &str, name: &str) -> Box<Bucket> {
    match Bucket::new(name, region(endpoint), credentials()) {
        Ok(bucket) => bucket.with_path_style(),
        Err(_) => panic!("cluster_bucket_client_invalid"),
    }
}

async fn s3_call<T, F>(future: F) -> AnyResult<T>
where
    F: Future<Output = Result<T, s3::error::S3Error>>,
{
    match tokio::time::timeout(S3_TIMEOUT, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(_)) => Err(anyhow!("s3_operation_failed")),
        Err(_) => Err(anyhow!("s3_operation_timeout")),
    }
}

fn unique_bucket(scenario: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("cluster_bucket_clock_invalid")
        .as_nanos();
    let counter = BUCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = format!("cl-{scenario}-{}-{nanos:x}-{counter:x}", std::process::id());
    assert!(name.len() <= 63, "cluster_bucket_name_too_long");
    name
}

async fn create_bucket(endpoint: &str, scenario: &str) -> (String, Box<Bucket>) {
    let name = unique_bucket(scenario);
    let response = match s3_call(Bucket::create_with_path_style(
        &name,
        region(endpoint),
        credentials(),
        BucketConfiguration::default(),
    ))
    .await
    {
        Ok(response) => response,
        Err(_) => panic!("s3_create_bucket_failed"),
    };
    assert!(response.response_code == 200, "s3_create_bucket_status");
    (name.clone(), bucket_at(endpoint, &name))
}

fn etag(headers: &std::collections::HashMap<String, String>) -> String {
    let value = headers
        .get("etag")
        .or_else(|| headers.get("ETag"))
        .or_else(|| headers.get("e-tag"));
    let value = match value {
        Some(value) => value,
        None => panic!("s3_put_etag_missing"),
    };
    let value = value.trim_matches('"').to_owned();
    assert!(!value.is_empty(), "s3_put_etag_empty");
    value
}

fn cluster_client(name: &str) -> ClusterClient {
    match ClusterClient::new(&endpoint(name)) {
        Ok(client) => client,
        Err(_) => panic!("cluster_client_configuration_invalid"),
    }
}

fn expect_cluster_result<T>(
    result: Result<T, cluster_support::ProbeError>,
    category: &'static str,
) -> T {
    match result {
        Ok(value) => value,
        Err(error) => panic!("{category}: {error}"),
    }
}

fn expect_local_kubo(result: AnyResult<Vec<u8>>, category: &'static str) -> Vec<u8> {
    match result {
        Ok(value) => value,
        Err(_) => panic!("{category}"),
    }
}

#[tokio::test]
async fn cluster_topology_converges() {
    let cluster_a = cluster_client("IPFS_S3_CLUSTER_A_REST_URL");
    let cluster_b = cluster_client("IPFS_S3_CLUSTER_B_REST_URL");
    let peers = expect_cluster_result(
        wait_for_shared_two_peer_view(&cluster_a, &cluster_b, CONVERGENCE_TIMEOUT).await,
        "topology_convergence_failed",
    );
    assert!(peers.len() == 2, "topology_peer_count_invalid");
    println!("peers=2 version=1.1.6");
}

#[tokio::test]
async fn cluster_proxy_compatibility() {
    let result: AnyResult<()> = match tokio::time::timeout(PROXY_SEQUENCE_TIMEOUT, async {
        let proxy = endpoint("IPFS_S3_CLUSTER_A_PROXY_URL");
        ensure!(
            proxy == "http://127.0.0.1:59103",
            "proxy_endpoint_not_exact_validation_loopback"
        );
        let client = KuboClient::new_with_timeouts(
            proxy,
            PROXY_CONTROL_TIMEOUT,
            PROXY_DOWNLOAD_IDLE_TIMEOUT,
        );
        let body = Bytes::from_static(b"cluster-proxy-add-pin-cat-compatibility-v1");
        let split = body.len() / 2;
        let source = futures_util::stream::iter(vec![
            Ok::<Bytes, std::io::Error>(body.slice(..split)),
            Ok::<Bytes, std::io::Error>(body.slice(split..)),
        ]);
        let cid = stream_add(&client, source, 1)
            .await
            .map_err(|_| anyhow!("production_stream_add_failed"))?;
        pin_add(&client, &cid)
            .await
            .map_err(|_| anyhow!("production_pin_add_failed"))?;
        let cat = stream_cat(&client, &cid, None)
            .await
            .map_err(|_| anyhow!("production_stream_cat_failed"))?;
        tokio::pin!(cat);
        let mut actual = Vec::new();
        while let Some(chunk) = cat.next().await {
            let chunk = chunk.map_err(|_| anyhow!("production_stream_cat_chunk_failed"))?;
            ensure!(
                actual.len().saturating_add(chunk.len()) <= body.len(),
                "production_stream_cat_length_mismatch"
            );
            actual.extend_from_slice(&chunk);
        }
        ensure!(
            actual.as_slice() == body.as_ref(),
            "production_stream_cat_bytes_mismatch"
        );
        Ok(())
    })
    .await
    {
        Ok(inner) => inner,
        Err(_) => Err(anyhow!("proxy_compatibility_sequence_timeout")),
    };
    if result.is_err() {
        panic!("proxy_compatibility_failed");
    }
    println!("proxy_add_pin_cat=true");
}

#[tokio::test]
async fn cluster_replication_and_retention() {
    let cluster_a = cluster_client("IPFS_S3_CLUSTER_A_REST_URL");
    let cluster_b = cluster_client("IPFS_S3_CLUSTER_B_REST_URL");
    let peers = expect_cluster_result(
        wait_for_shared_two_peer_view(&cluster_a, &cluster_b, CONVERGENCE_TIMEOUT).await,
        "replication_topology_convergence_failed",
    );
    let gateway = endpoint("IPFS_S3_CLUSTER_GATEWAY_ENDPOINT");
    let (_bucket_name, bucket) = create_bucket(&gateway, "repl").await;
    let body = b"ipfs3-cluster-retained-replication-v1";
    let put = match s3_call(bucket.put_object("retained.bin", body)).await {
        Ok(response) => response,
        Err(_) => panic!("s3_replication_put_failed"),
    };
    assert!(put.status_code() == 200, "s3_replication_put_status");
    let cid = etag(&put.headers());
    let get = match s3_call(bucket.get_object("retained.bin")).await {
        Ok(response) => response,
        Err(_) => panic!("s3_replication_get_failed"),
    };
    assert!(get.status_code() == 200, "s3_replication_get_status");
    assert!(
        get.bytes().as_ref() == body,
        "s3_replication_get_body_mismatch"
    );

    let evidence_a = expect_cluster_result(
        wait_for_two_pinned(&cluster_a, &cid, &peers, CONVERGENCE_TIMEOUT).await,
        "cluster_a_two_pin_evidence_failed",
    );
    let evidence_b = expect_cluster_result(
        wait_for_two_pinned(&cluster_b, &cid, &peers, CONVERGENCE_TIMEOUT).await,
        "cluster_b_two_pin_evidence_failed",
    );
    assert!(
        evidence_a.allocations == peers && evidence_a.pinned_peers == peers,
        "cluster_a_two_pin_evidence_invalid"
    );
    assert!(
        evidence_b.allocations == peers && evidence_b.pinned_peers == peers,
        "cluster_b_two_pin_evidence_invalid"
    );

    let kubo_a = endpoint("IPFS_S3_CLUSTER_KUBO_A_URL");
    let kubo_b = endpoint("IPFS_S3_CLUSTER_KUBO_B_URL");
    assert!(
        expect_local_kubo(kubo_cat(&kubo_a, &cid).await, "kubo_a_cat_failed") == body,
        "kubo_a_cat_body_mismatch"
    );
    assert!(
        expect_local_kubo(kubo_cat(&kubo_b, &cid).await, "kubo_b_cat_failed") == body,
        "kubo_b_cat_body_mismatch"
    );

    let deleted = match s3_call(bucket.delete_object("retained.bin")).await {
        Ok(response) => response,
        Err(_) => panic!("s3_replication_delete_failed"),
    };
    assert!(deleted.status_code() == 204, "s3_replication_delete_status");
    let head = match tokio::time::timeout(
        S3_TIMEOUT,
        send_sigv4(
            Method::HEAD,
            &gateway,
            &bucket.name(),
            "retained.bin",
            &[],
            Vec::new(),
            HeaderMap::new(),
            "test",
        ),
    )
    .await
    {
        Ok(response) => response,
        Err(_) => panic!("s3_signed_head_timeout"),
    };
    assert!(
        head.status().as_u16() == 404,
        "s3_signed_head_not_found_status"
    );
    let bucket_deleted = match s3_call(bucket.delete()).await {
        Ok(status) => status,
        Err(_) => panic!("s3_bucket_delete_failed"),
    };
    assert!(bucket_deleted == 204, "s3_bucket_delete_status");

    let allocation = match cluster_a.allocation_probe(&cid).await {
        Ok(Some(allocation)) => allocation,
        Ok(None) | Err(_) => panic!("retained_allocation_not_available"),
    };
    assert!(
        allocation.cid == cid
            && allocation.replication_factor_min == 2
            && allocation.replication_factor_max == 2
            && expect_cluster_result(
                peer_set_digest(&allocation.allocations),
                "retained_allocation_peer_digest_failed",
            ) == expect_cluster_result(peer_set_digest(&peers), "retained_peer_digest_failed"),
        "retained_allocation_contract_invalid"
    );
    let retained_evidence = expect_cluster_result(
        wait_for_two_pinned(&cluster_a, &cid, &peers, CONVERGENCE_TIMEOUT).await,
        "retained_two_pin_evidence_failed",
    );
    assert!(
        retained_evidence.allocations == peers && retained_evidence.pinned_peers == peers,
        "retained_two_pin_evidence_invalid"
    );
    assert!(
        expect_local_kubo(kubo_cat(&kubo_b, &cid).await, "retained_kubo_b_cat_failed") == body,
        "retained_kubo_b_cat_body_mismatch"
    );

    let state = RecoveryState {
        schema: "ipfs3-cluster-recovery-v1".to_owned(),
        source: "s3-replication-retention-v1".to_owned(),
        cid,
        body: body.to_vec(),
        peer_set_sha256: expect_cluster_result(
            peer_set_digest(&peers),
            "recovery_peer_digest_failed",
        ),
    };
    if state.write_claimed(&state_path()).is_err() {
        panic!("recovery_state_write_failed");
    }
}

#[tokio::test]
async fn cluster_peer_b_outage_contract() {
    let state = match RecoveryState::read(&state_path()) {
        Ok(state) => state,
        Err(_) => panic!("recovery_state_read_failed"),
    };
    let cluster_a = cluster_client("IPFS_S3_CLUSTER_A_REST_URL");
    expect_cluster_result(
        wait_until_not_fully_pinned(
            &cluster_a,
            &state.cid,
            &state.peer_set_sha256,
            Duration::from_secs(90),
        )
        .await,
        "peer_b_outage_evidence_failed",
    );
    let kubo_a = endpoint("IPFS_S3_CLUSTER_KUBO_A_URL");
    assert!(
        expect_local_kubo(
            kubo_cat(&kubo_a, &state.cid).await,
            "outage_kubo_a_cat_failed"
        ) == state.body,
        "outage_kubo_a_cat_body_mismatch"
    );
}

#[tokio::test]
async fn cluster_peer_b_restart_recovery() {
    let state = match RecoveryState::read(&state_path()) {
        Ok(state) => state,
        Err(_) => panic!("recovery_state_read_failed"),
    };
    let cluster_a = cluster_client("IPFS_S3_CLUSTER_A_REST_URL");
    let cluster_b = cluster_client("IPFS_S3_CLUSTER_B_REST_URL");
    let peers = expect_cluster_result(
        wait_for_shared_two_peer_view(&cluster_a, &cluster_b, CONVERGENCE_TIMEOUT).await,
        "recovery_topology_convergence_failed",
    );
    assert!(
        expect_cluster_result(peer_set_digest(&peers), "recovery_peer_digest_failed")
            == state.peer_set_sha256,
        "recovery_peer_digest_mismatch"
    );
    let evidence_a = expect_cluster_result(
        wait_for_two_pinned(&cluster_a, &state.cid, &peers, CONVERGENCE_TIMEOUT).await,
        "recovery_cluster_a_two_pin_evidence_failed",
    );
    let evidence_b = expect_cluster_result(
        wait_for_two_pinned(&cluster_b, &state.cid, &peers, CONVERGENCE_TIMEOUT).await,
        "recovery_cluster_b_two_pin_evidence_failed",
    );
    assert!(
        evidence_a.allocations == peers && evidence_a.pinned_peers == peers,
        "recovery_cluster_a_two_pin_evidence_invalid"
    );
    assert!(
        evidence_b.allocations == peers && evidence_b.pinned_peers == peers,
        "recovery_cluster_b_two_pin_evidence_invalid"
    );
    let kubo_b = endpoint("IPFS_S3_CLUSTER_KUBO_B_URL");
    assert!(
        expect_local_kubo(
            kubo_cat(&kubo_b, &state.cid).await,
            "recovery_kubo_b_cat_failed"
        ) == state.body,
        "recovery_kubo_b_cat_body_mismatch"
    );
}
