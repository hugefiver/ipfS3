use ipfs_s3_gateway::pinning;

// Temporary standalone leaf wiring. Remove this #[path] when Stage 6 registers
// pinning::cluster; its module-local contract tests then run via cargo test --lib.
#[path = "../src/pinning/cluster/mod.rs"]
#[allow(dead_code, unused_imports)] // The standalone verifier does not exercise the write leaf.
mod cluster;

#[path = "support/cluster_pinning_real_fixture.rs"]
mod real_fixture;

/// Read-only leaf acceptance against an explicitly selected, disposable fixture.
/// This is not the Stage 6 provider/worker, CAR or disaster-recovery acceptance.
#[tokio::test]
#[ignore = "requires tests/run-cluster-pinning-real.ps1 and explicit fixture environment"]
async fn cluster_replica_verifier_real_two_peer_lifecycle() -> anyhow::Result<()> {
    use anyhow::ensure;
    use cluster::{
        ClusterNetworkPolicy, ClusterPinset, ClusterReplicaVerifier, ClusterRestProfile,
        ReplicaOutcome, RestLimits, VerificationCode,
    };
    use std::{collections::BTreeSet, time::Duration};

    // Running --ignored without ALL required environment is an error, never a skip.
    let fixture = real_fixture::RealClusterFixture::from_environment().await?;
    fixture.validate_nodes().await?;
    let pinset = ClusterPinset::new("isolated-real-cluster", fixture.run_id())?;
    let limits = RestLimits {
        total_timeout: Duration::from_secs(15),
        idle_timeout: Duration::from_secs(5),
        max_observation_age: Duration::from_secs(300),
        ..RestLimits::default()
    };
    let profile_a = ClusterRestProfile::new_with_network_policy(
        pinset.clone(),
        Some(fixture.rest_a()),
        None,
        2,
        limits,
        ClusterNetworkPolicy::AllowPrivate,
    )?;
    let profile_b = ClusterRestProfile::new_with_network_policy(
        pinset,
        Some(fixture.rest_b()),
        None,
        2,
        limits,
        ClusterNetworkPolicy::AllowPrivate,
    )?;
    let verifier_a = ClusterReplicaVerifier::new(profile_a.clone())?;
    let verifier_b = ClusterReplicaVerifier::new(profile_b.clone())?;

    // A single raw SHA-256 block is the complete content for this fixture CID.
    // No Kubo swarm listeners/peers: B cannot fetch it from A behind our back.
    let payload = format!(
        "Cluster v1.1.6 leaf replica acceptance: {}\n",
        fixture.run_id()
    );
    let cid = fixture.preseed_a(payload.as_bytes()).await?;
    fixture.assert_local_absent_on_b(&cid).await?;
    fixture.pin_in_cluster(&cid).await?;
    let one = fixture.wait_for_pinned(fixture.rest_a(), &cid, 1).await?;
    ensure!(one.pinned_peers() == BTreeSet::from([fixture.peer_a().to_owned()]));
    fixture.assert_local_absent_on_b(&cid).await?;
    let insufficient = verifier_a.observe(&cid).await;
    ensure!(insufficient.outcome() == ReplicaOutcome::Insufficient);
    let evidence = insufficient
        .evidence()
        .ok_or_else(|| anyhow::anyhow!("missing 1/2 evidence"))?;
    ensure!(evidence.required_replicas == 2);
    ensure!(evidence.allocation.peers == fixture.expected_peers());
    ensure!(evidence.pinned_peers == one.pinned_peers());
    fixture.record("one-of-two-leaf", &format!("{insufficient:?}"))?;

    fixture.preseed_b(&cid, payload.as_bytes()).await?;
    fixture.recover_b(&cid).await?;
    fixture.wait_for_pinned(fixture.rest_a(), &cid, 2).await?;
    fixture.wait_for_pinned(fixture.rest_b(), &cid, 2).await?;
    fixture
        .assert_local_complete(&cid, payload.as_bytes())
        .await?;
    let complete = verifier_a.observe(&cid).await;
    let complete_clone = complete.clone();
    ensure!(complete.outcome() == ReplicaOutcome::Complete);
    ensure!(complete.evidence().unwrap().pinned_peers == fixture.expected_peers());
    ensure!(insufficient.outcome() == ReplicaOutcome::Unknown);
    let other_address = verifier_b.observe(&cid).await;
    ensure!(other_address.outcome() == ReplicaOutcome::Complete);
    ensure!(other_address.evidence().unwrap().pinned_peers == fixture.expected_peers());

    // Assert actual leaf resource identity, not an invented worker aggregation.
    // Two REST addresses are ONE logical pinset, not two independent domains.
    let key_a = profile_a.resource_key(&cid)?;
    let key_b = profile_b.resource_key(&cid)?;
    ensure!(key_a == key_b);
    ensure!(complete.evidence().unwrap().resource == key_a);
    ensure!(other_address.evidence().unwrap().resource == key_b);
    ensure!(BTreeSet::from([key_a, key_b]).len() == 1);
    fixture.record("two-of-two-leaf", &format!("{complete:?}"))?;
    fixture.record("same-pinset-second-rest", &format!("{other_address:?}"))?;

    // Stop only the allocated B Kubo owned by this run; Cluster B remains online
    // so its fresh tracker read cannot be mistaken for a lost REST endpoint.
    fixture.stop_owned_kubo_b().await?;
    let degraded = fixture.wait_for_pinned(fixture.rest_a(), &cid, 1).await?;
    ensure!(degraded.pinned_peers() == BTreeSet::from([fixture.peer_a().to_owned()]));
    // Rule out expiry as the reason for invalidation in this case.
    ensure!(
        complete.outcome() == ReplicaOutcome::Complete,
        "proof expired before refresh"
    );
    let refreshed = verifier_a.observe(&cid).await;
    ensure!(refreshed.outcome() != ReplicaOutcome::Complete);
    ensure!(complete.outcome() == ReplicaOutcome::Unknown);
    ensure!(complete_clone.outcome() == ReplicaOutcome::Unknown);
    ensure!(complete.failure().unwrap().code == VerificationCode::StaleObservation);
    ensure!(
        complete.evidence().unwrap().pinned_peers.len() == 2,
        "historical evidence was erased"
    );
    fixture.record("outage-fresh-leaf", &format!("{refreshed:?}"))?;
    fixture.record("superseded-proof", &format!("{complete:?}"))?;
    fixture.record("acceptance", "PASS: 1/2, 2/2, local raw bytes, refresh after allocated-node outage, same-pinset identity")?;
    Ok(())
}
