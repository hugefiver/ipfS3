//! Independent IPFS Cluster v1.1.6 provider and fresh REST replica verification.

mod dto;
mod error;
pub(crate) mod network;
mod observation;
mod peer;
mod profile;
pub mod provider;
mod transport;
mod verifier;

pub use error::{RestOperation, VerificationCode, VerificationFailure};
pub use network::ClusterNetworkPolicy;
pub use observation::{
    AllocationBinding, PeerReplicaState, ReplicaEvidence, ReplicaObservation, ReplicaOutcome,
};
pub use profile::{ClusterAuth, ClusterPinset, ClusterResourceKey, ClusterRestProfile, RestLimits};
pub use verifier::ClusterReplicaVerifier;

#[cfg(test)]
mod freshness_tests;
#[cfg(test)]
mod identity_tests;
#[cfg(test)]
mod network_tests;
#[cfg(test)]
pub(crate) mod network_tls_tests;
#[cfg(test)]
mod protocol_tests;
#[cfg(test)]
mod stream_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod transport_tests;
