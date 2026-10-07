//! Opt-in, isolated provider transport acceptance. Not full Stage6 coverage.
use ipfs_s3_gateway::pinning;

#[path = "support/cluster_provider_real_car.rs"]
mod car;
#[path = "../src/pinning/cluster/mod.rs"]
#[allow(dead_code, unused_imports)]
mod cluster;
#[path = "support/cluster_pinning_real_fixture.rs"]
mod cluster_pinning_real_fixture;
#[path = "support/cluster_provider_real_contract.rs"]
mod provider_contract;
#[path = "support/cluster_provider_real.rs"]
mod provider_fixture;
#[path = "support/cluster_provider_real_crypto.rs"]
mod stored_crypto;
#[path = "support/cluster_provider_real_stream.rs"]
mod stream_contract;

#[tokio::test]
#[ignore = "requires explicit provider-phase env and run-cluster-pinning-real.ps1 -ProviderPhase"]
async fn cluster_provider_real_independent_source_car() {
    provider_fixture::acceptance().await.unwrap();
}
