use super::{tests::*, *};
use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[tokio::test]
async fn cid_aliases_are_equivalent_but_changed_codec_or_invalid_cid_is_not() {
    let parsed = ::cid::Cid::try_from(CID).unwrap();
    let canonical = ::cid::Cid::new_v1(parsed.codec(), *parsed.hash()).to_string();
    let server = MockServer::start().await;
    let mut alloc = allocation(&["a", "b"]);
    let mut status = global(&["a", "b"], &[("a", "pinned"), ("b", "pinned")]);
    alloc["cid"] = json!(canonical);
    status["cid"] = json!(canonical);
    mounts(&server, alloc, status).await;
    let p = profile(&server.uri(), 2);
    assert_eq!(
        p.resource_key(CID).unwrap(),
        p.resource_key(&canonical).unwrap()
    );
    let verifier = ClusterReplicaVerifier::new(p).unwrap();
    assert_eq!(
        verifier.observe(CID).await.outcome(),
        ReplicaOutcome::Complete
    );
    let raw = ::cid::Cid::new_v1(0x55, *parsed.hash()).to_string();
    let mut wrong = allocation(&["a", "b"]);
    wrong["cid"] = json!(raw);
    Mock::given(method("GET"))
        .and(path(format!("/allocations/{CID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(wrong))
        .with_priority(1)
        .mount(&server)
        .await;
    let mismatch = verifier.observe(CID).await;
    assert_eq!(
        mismatch.failure().unwrap().code,
        VerificationCode::CidMismatch
    );
    let count = server.received_requests().await.unwrap().len();
    assert_eq!(
        verifier
            .observe("invalid/token-sentinel")
            .await
            .failure()
            .unwrap()
            .code,
        VerificationCode::InvalidCid
    );
    assert_eq!(server.received_requests().await.unwrap().len(), count);
}

#[tokio::test]
async fn duplicate_peer_map_keys_and_unknown_status_are_not_cherry_picked() {
    let server = MockServer::start().await;
    mounts(
        &server,
        allocation(&["a", "b"]),
        global(&["a", "b"], &[("a", "pinned"), ("b", "pinned")]),
    )
    .await;
    let a = peer("a");
    let b = peer("b");
    let duplicate = format!(
        r#"{{"cid":"{CID}","allocations":["{a}","{b}"],"created":"{}","peer_map":{{"{a}":{{"status":"pin_error","timestamp":"{}","error":"token-sentinel"}},"{a}":{{"status":"pinned","timestamp":"{}","error":""}}}}}}"#,
        created(),
        created(),
        created()
    );
    let mut unknown = global(
        &["a", "b"],
        &[("a", "pinned"), ("b", "unknown-token-sentinel")],
    );
    unknown["peer_map"][peer("b")]["timestamp"] = json!(created());
    for body in [duplicate, unknown.to_string()] {
        Mock::given(method("GET"))
            .and(path(format!("/pins/{CID}")))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .with_priority(1)
            .mount(&server)
            .await;
        let observed = ClusterReplicaVerifier::new(profile(&server.uri(), 2))
            .unwrap()
            .observe(CID)
            .await;
        assert_eq!(
            observed.failure().unwrap().code,
            VerificationCode::MalformedResponse
        );
        assert!(!format!("{observed:?}").contains("token-sentinel"));
        server.reset().await;
        mounts(
            &server,
            allocation(&["a", "b"]),
            global(&["a", "b"], &[("a", "pinned"), ("b", "pinned")]),
        )
        .await;
    }
}

#[tokio::test]
async fn allocation_set_order_and_duplicates_are_not_revisions_or_extra_replicas() {
    let server = MockServer::start().await;
    mounts(
        &server,
        allocation(&["a", "b", "b"]),
        global(&["b", "a", "a"], &[("a", "pinned"), ("b", "pinned")]),
    )
    .await;
    let result = ClusterReplicaVerifier::new(profile(&server.uri(), 2))
        .unwrap()
        .observe(CID)
        .await;
    assert_eq!(result.outcome(), ReplicaOutcome::Complete);
    assert_eq!(result.evidence().unwrap().allocation.peers.len(), 2);
    assert_eq!(result.evidence().unwrap().pinned_peers.len(), 2);
}

#[tokio::test]
async fn two_text_encodings_of_the_same_peer_do_not_count_as_two_replicas() {
    let server = MockServer::start().await;
    let digest = [b'a'; 32];
    let hash = ::cid::multihash::Multihash::<64>::wrap(0x12, &digest).unwrap();
    let alias = ::cid::Cid::new_v1(0x72, hash).to_string();
    let mut alloc = allocation(&["a"]);
    alloc["allocations"] = json!([peer("a"), alias]);
    let mut status = global(&["a"], &[("a", "pinned")]);
    // The official GlobalPinInfo map uses peer.String(): canonical base58 IDs.
    status["allocations"] = json!([alias, peer("a")]);
    mounts(&server, alloc, status).await;
    let observed = ClusterReplicaVerifier::new(profile(&server.uri(), 2))
        .unwrap()
        .observe(CID)
        .await;
    assert_eq!(observed.outcome(), ReplicaOutcome::Insufficient);
    assert_eq!(observed.evidence().unwrap().allocation.peers.len(), 1);
    assert_eq!(observed.evidence().unwrap().pinned_peers.len(), 1);
}

#[tokio::test]
async fn all_peers_mode_does_not_infer_allocations_from_peer_map() {
    let server = MockServer::start().await;
    let mut alloc = allocation(&[]);
    alloc["allocations"] = json!(null);
    alloc["replication_factor_min"] = json!(-1);
    alloc["replication_factor_max"] = json!(-1);
    let mut status = global(&[], &[("a", "pinned"), ("b", "pinned")]);
    status["allocations"] = json!(null);
    mounts(&server, alloc, status).await;
    let result = ClusterReplicaVerifier::new(profile(&server.uri(), 2))
        .unwrap()
        .observe(CID)
        .await;
    assert_eq!(result.outcome(), ReplicaOutcome::Insufficient);
    assert!(result.evidence().unwrap().pinned_peers.is_empty());
}

#[tokio::test]
async fn each_missing_required_dto_field_is_unknown_not_empty_or_pinned() {
    for field in ["cid", "allocations", "created", "peer_map"] {
        let server = MockServer::start().await;
        let mut status = global(&["a", "b"], &[("a", "pinned"), ("b", "pinned")]);
        status.as_object_mut().unwrap().remove(field);
        mounts(&server, allocation(&["a", "b"]), status).await;
        let result = ClusterReplicaVerifier::new(profile(&server.uri(), 2))
            .unwrap()
            .observe(CID)
            .await;
        assert_eq!(
            result.failure().unwrap().code,
            VerificationCode::MalformedResponse
        );
    }
}
