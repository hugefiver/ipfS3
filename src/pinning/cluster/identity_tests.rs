use super::{tests::*, *};
use serde_json::json;
use std::time::Duration;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[tokio::test]
async fn same_pinset_at_two_addresses_has_one_resource_and_the_same_peer_set() {
    let a = MockServer::start().await;
    let b = MockServer::start().await;
    for server in [&a, &b] {
        mounts(
            server,
            allocation(&["a", "b"]),
            global(&["a", "b"], &[("a", "pinned"), ("b", "pinned")]),
        )
        .await;
    }
    let left = ClusterReplicaVerifier::new(profile(&a.uri(), 2))
        .unwrap()
        .observe(CID)
        .await;
    let right = ClusterReplicaVerifier::new(profile(&b.uri(), 2))
        .unwrap()
        .observe(CID)
        .await;
    assert_eq!(left.outcome(), ReplicaOutcome::Complete);
    assert_eq!(right.outcome(), ReplicaOutcome::Complete);
    let left = left.evidence().unwrap();
    let right = right.evidence().unwrap();
    assert_eq!(left.resource, right.resource);
    assert_eq!(left.pinned_peers, right.pinned_peers);
    assert_eq!(left.pinned_peers.union(&right.pinned_peers).count(), 2); // never 4
}

#[tokio::test]
async fn another_cid_does_not_invalidate_an_unrelated_current_observation() {
    let server = MockServer::start().await;
    mounts(
        &server,
        allocation(&["a", "b"]),
        global(&["a", "b"], &[("a", "pinned"), ("b", "pinned")]),
    )
    .await;
    let verifier = ClusterReplicaVerifier::new(profile(&server.uri(), 2)).unwrap();
    let first = verifier.observe(CID).await;
    let other = verifier
        .observe("bafkreigh2akiscaildcw4535x3w2mfj6tcjv32hzy2s5h5mffqezk4cuyq")
        .await;
    assert_eq!(other.outcome(), ReplicaOutcome::Unknown);
    assert_eq!(first.outcome(), ReplicaOutcome::Complete);
}

#[tokio::test]
async fn slow_valid_response_cannot_mint_a_fresh_proof_after_the_observation_window() {
    let server = MockServer::start().await;
    mounts(
        &server,
        allocation(&["a", "b"]),
        global(&["a", "b"], &[("a", "pinned"), ("b", "pinned")]),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/version"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"version": "1.1.6+build-7"}))
                .set_delay(Duration::from_millis(100)),
        )
        .with_priority(1)
        .mount(&server)
        .await;
    let p = ClusterRestProfile::new_with_network_policy(
        ClusterPinset::new("cluster-backend", "pinset-a").unwrap(),
        Some(&server.uri()),
        None,
        2,
        RestLimits {
            max_observation_age: Duration::from_millis(50),
            ..RestLimits::default()
        },
        ClusterNetworkPolicy::AllowPrivate,
    )
    .unwrap();
    let observed = ClusterReplicaVerifier::new(p).unwrap().observe(CID).await;
    assert_eq!(observed.outcome(), ReplicaOutcome::Unknown);
    assert_eq!(
        observed.failure().unwrap().code,
        VerificationCode::StaleObservation
    );
    assert_eq!(observed.evidence().unwrap().pinned_peers.len(), 2); // historical only
}
