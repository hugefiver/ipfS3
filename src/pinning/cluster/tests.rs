use super::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

pub(super) const CID: &str = "QmYwAPJzv5CZsnAzt8auVZRnGiRAz1F7TaqzM9zECsFcdX";

pub(super) fn created() -> String {
    "2026-09-20T00:00:00Z".into()
}

pub(super) fn peer(label: &str) -> String {
    let digest = [label.as_bytes()[0]; 32];
    let hash = ::cid::multihash::Multihash::<64>::wrap(0x12, &digest).unwrap();
    ::cid::multibase::encode(::cid::multibase::Base::Base58Btc, hash.to_bytes())[1..].to_owned()
}

pub(super) fn allocation(peers: &[&str]) -> Value {
    let peers = peers.iter().map(|id| peer(id)).collect::<Vec<_>>();
    json!({"cid": CID, "allocations": peers, "timestamp": created(),
        "replication_factor_min": 2, "replication_factor_max": 2})
}

pub(super) fn global(allocations: &[&str], peers: &[(&str, &str)]) -> Value {
    let map = peers
        .iter()
        .map(|(id, status)| {
            (
                peer(id),
                json!({
                    "status": status, "timestamp": created(), "error": ""
                }),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let allocations = allocations.iter().map(|id| peer(id)).collect::<Vec<_>>();
    json!({"cid": CID, "created": created(), "allocations": allocations, "peer_map": map})
}

pub(super) fn profile(endpoint: &str, min: usize) -> ClusterRestProfile {
    ClusterRestProfile::new_with_network_policy(
        ClusterPinset::new("cluster-backend", "pinset-a").unwrap(),
        Some(endpoint),
        None,
        min,
        RestLimits::default(),
        ClusterNetworkPolicy::AllowPrivate, // Explicit loopback fixture opt-in.
    )
    .unwrap()
}

pub(super) async fn mounts(server: &MockServer, alloc: Value, status: Value) {
    Mock::given(method("GET"))
        .and(path("/version"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"version": "1.1.6"})))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/allocations/{CID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(alloc))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/pins/{CID}")))
        .and(query_param("local", "false"))
        .respond_with(ResponseTemplate::new(200).set_body_json(status))
        .mount(server)
        .await;
}

#[tokio::test]
async fn only_distinct_allocated_pinned_peers_count() {
    for (alloc, peers, outcome, count) in [
        (
            vec!["a", "b"],
            vec![("a", "pinned"), ("b", "pinning")],
            ReplicaOutcome::Insufficient,
            1,
        ),
        (
            vec!["a", "b"],
            vec![("a", "pinned"), ("b", "pinned")],
            ReplicaOutcome::Complete,
            2,
        ),
        (
            vec!["a", "b"],
            vec![("a", "pinned"), ("b", "remote"), ("c", "pinned")],
            ReplicaOutcome::Insufficient,
            1,
        ),
        (
            vec!["a", "a"],
            vec![("a", "pinned"), ("c", "pinned")],
            ReplicaOutcome::Insufficient,
            1,
        ),
        (
            vec!["a", "b"],
            vec![("a", "pinned")],
            ReplicaOutcome::Insufficient,
            1,
        ),
        (
            vec!["a", "b"],
            vec![("a", "pin_error"), ("b", "cluster_error")],
            ReplicaOutcome::Insufficient,
            0,
        ),
        (
            vec!["a", "b"],
            vec![("a", "unpinned"), ("b", "unpinning")],
            ReplicaOutcome::Insufficient,
            0,
        ),
    ] {
        let server = MockServer::start().await;
        mounts(&server, allocation(&alloc), global(&alloc, &peers)).await;
        let observed = ClusterReplicaVerifier::new(profile(&server.uri(), 2))
            .unwrap()
            .observe(CID)
            .await;
        assert_eq!(observed.outcome(), outcome);
        assert_eq!(observed.evidence().unwrap().pinned_peers.len(), count);
        assert_eq!(server.received_requests().await.unwrap().len(), 4);
    }
}

#[tokio::test]
async fn pinned_with_free_error_or_old_peer_revision_is_not_counted() {
    for (field, value, expected) in [
        (
            "error",
            "token-and-endpoint-sentinel",
            PeerReplicaState::Error,
        ),
        ("timestamp", "2026-09-19T00:00:00Z", PeerReplicaState::Stale),
    ] {
        let server = MockServer::start().await;
        let mut status = global(&["a", "b"], &[("a", "pinned"), ("b", "pinned")]);
        status["peer_map"][peer("b")][field] = json!(value);
        mounts(&server, allocation(&["a", "b"]), status).await;
        let observed = ClusterReplicaVerifier::new(profile(&server.uri(), 2))
            .unwrap()
            .observe(CID)
            .await;
        assert_eq!(observed.outcome(), ReplicaOutcome::Insufficient);
        assert_eq!(
            observed.evidence().unwrap().peer_states[&peer("b")],
            expected
        );
        assert!(!format!("{observed:?}").contains("token-and-endpoint-sentinel"));
    }
}

#[tokio::test]
async fn global_cid_allocation_and_creation_must_match() {
    for field in ["cid", "allocations", "created"] {
        let server = MockServer::start().await;
        let mut status = global(&["a", "b"], &[("a", "pinned"), ("b", "pinned")]);
        status[field] = match field {
            "cid" => json!("bafkreigh2akiscaildcw4535x3w2mfj6tcjv32hzy2s5h5mffqezk4cuyq"),
            "allocations" => json!([peer("a"), peer("c")]),
            _ => json!("2026-09-19T00:00:00Z"),
        };
        mounts(&server, allocation(&["a", "b"]), status).await;
        let observed = ClusterReplicaVerifier::new(profile(&server.uri(), 2))
            .unwrap()
            .observe(CID)
            .await;
        assert_eq!(observed.outcome(), ReplicaOutcome::Unknown);
        assert!(observed.evidence().is_none());
    }
}

#[tokio::test]
async fn min_replicas_is_only_a_confirmation_requirement() {
    let server = MockServer::start().await;
    mounts(
        &server,
        allocation(&["a", "b"]),
        global(&["a", "b"], &[("a", "pinned"), ("b", "remote")]),
    )
    .await;
    let observed = ClusterReplicaVerifier::new(profile(&server.uri(), 1))
        .unwrap()
        .observe(CID)
        .await;
    assert_eq!(observed.outcome(), ReplicaOutcome::Complete);
    assert_eq!(
        observed
            .evidence()
            .unwrap()
            .allocation
            .replication_factor_min,
        2
    );
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| r.method == "GET")
    );
}

#[test]
fn rest_configuration_is_required_and_identity_does_not_use_address() {
    let pinset = ClusterPinset::new("cluster-backend", "pinset-a").unwrap();
    let missing =
        ClusterRestProfile::new(pinset, None, None, 2, RestLimits::default()).unwrap_err();
    assert_eq!(missing.code, VerificationCode::MissingRestConfiguration);
    let a = profile("http://127.0.0.1:9001", 2);
    let b = profile("http://127.0.0.1:9002", 2);
    assert_eq!(a.resource_key(CID).unwrap(), b.resource_key(CID).unwrap());
    assert!(!format!("{a:?}").contains("127.0.0.1"));
}
