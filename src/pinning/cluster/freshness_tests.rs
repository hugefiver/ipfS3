use super::{tests::*, *};
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

#[tokio::test]
async fn allocation_changes_on_either_side_require_a_whole_new_observation() {
    for change in ["peers", "timestamp", "factor"] {
        let server = MockServer::start().await;
        let before = allocation(&["a", "b"]);
        let mut after = before.clone();
        match change {
            "peers" => after["allocations"] = json!([peer("a"), peer("c")]),
            "timestamp" => after["timestamp"] = json!("2026-09-21T00:00:00Z"),
            _ => after["replication_factor_min"] = json!(3),
        }
        mounts(
            &server,
            before.clone(),
            global(&["a", "b"], &[("a", "pinned"), ("b", "pinned")]),
        )
        .await;
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        Mock::given(method("GET"))
            .and(path(format!("/allocations/{CID}")))
            .respond_with(move |_: &wiremock::Request| {
                let data = if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    &before
                } else {
                    &after
                };
                ResponseTemplate::new(200).set_body_json(data)
            })
            .with_priority(1)
            .mount(&server)
            .await;
        let verifier = ClusterReplicaVerifier::new(profile(&server.uri(), 2)).unwrap();
        let observed = verifier.observe(CID).await;
        assert_eq!(observed.outcome(), ReplicaOutcome::Unknown);
        assert_eq!(
            observed.failure().unwrap().code,
            VerificationCode::AllocationChanged
        );
        assert!(observed.evidence().is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        server.reset().await;
        mounts(
            &server,
            allocation(&["a", "c"]),
            global(&["a", "c"], &[("a", "pinned"), ("c", "pinned")]),
        )
        .await;
        let fresh = verifier.observe(CID).await;
        assert_eq!(fresh.outcome(), ReplicaOutcome::Complete);
        assert!(fresh.evidence().unwrap().pinned_peers.contains(&peer("c")));
        assert_eq!(server.received_requests().await.unwrap().len(), 4);
    }
}

#[tokio::test]
async fn new_unknown_observation_invalidates_previous_complete_proof_without_erasing_history() {
    let server = MockServer::start().await;
    mounts(
        &server,
        allocation(&["a", "b"]),
        global(&["a", "b"], &[("a", "pinned"), ("b", "pinned")]),
    )
    .await;
    let verifier = ClusterReplicaVerifier::new(profile(&server.uri(), 2)).unwrap();
    let old = verifier.observe(CID).await;
    assert_eq!(old.outcome(), ReplicaOutcome::Complete);
    let historical = old.evidence().unwrap().clone();
    Mock::given(method("GET"))
        .and(path(format!("/pins/{CID}")))
        .respond_with(ResponseTemplate::new(503))
        .with_priority(1)
        .mount(&server)
        .await;
    let current = verifier.observe(CID).await;
    assert_eq!(current.outcome(), ReplicaOutcome::Unknown);
    assert_eq!(old.outcome(), ReplicaOutcome::Unknown);
    assert_eq!(
        old.failure().unwrap().code,
        VerificationCode::StaleObservation
    );
    assert_eq!(old.evidence().unwrap(), &historical);
}

#[tokio::test]
async fn proof_expiry_uses_local_observation_not_remote_creation_time() {
    let server = MockServer::start().await;
    mounts(
        &server,
        allocation(&["a", "b"]),
        global(&["a", "b"], &[("a", "pinned"), ("b", "pinned")]),
    )
    .await;
    let verifier = ClusterReplicaVerifier::new(profile(&server.uri(), 2)).unwrap();
    let observed = verifier.observe(CID).await;
    assert_eq!(observed.outcome(), ReplicaOutcome::Complete); // old creation timestamp is valid
    let evidence = observed.evidence().unwrap();
    assert!(evidence.started_at <= evidence.global_received_at);
    assert!(evidence.global_received_at <= evidence.finished_at);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;
    assert_eq!(observed.outcome(), ReplicaOutcome::Unknown);
    assert_eq!(observed.clone().outcome(), ReplicaOutcome::Unknown);
    assert!(observed.evidence().is_some());
}

#[tokio::test]
async fn local_query_or_local_shaped_reply_cannot_prove_global_replicas() {
    let server = MockServer::start().await;
    mounts(
        &server,
        allocation(&["a", "b"]),
        global(&["a", "b"], &[("a", "pinned")]),
    )
    .await;
    Mock::given(method("GET"))
        .and(path(format!("/pins/{CID}")))
        .and(query_param("local", "true"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(global(&["a", "b"], &[("a", "pinned"), ("b", "pinned")])),
        )
        .expect(0)
        .mount(&server)
        .await;
    let verifier = ClusterReplicaVerifier::new(profile(&server.uri(), 2)).unwrap();
    assert_eq!(
        verifier.observe(CID).await.outcome(),
        ReplicaOutcome::Insufficient
    );
    Mock::given(method("GET"))
        .and(path(format!("/pins/{CID}")))
        .and(query_param("local", "false"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "cid": CID, "allocations": [peer("a"), peer("b")], "created": created(),
            "peer": peer("a"), "status": "pinned", "timestamp": created(), "error": ""
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    assert_eq!(
        verifier.observe(CID).await.outcome(),
        ReplicaOutcome::Unknown
    );
}
