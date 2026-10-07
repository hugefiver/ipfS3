//! Cluster v1.1.6 write leaf. No registration, ownership grant or automatic repair.
mod add_response;
mod car;
mod cbor;
mod dagpb;
mod http;
mod observation;
mod options;
mod remove;
mod response;
mod stream;
mod submit;

pub use observation::{
    ClusterFailureCode, ClusterObservedResource, ClusterRemoteRef, ClusterResourceStatus,
    ClusterWriteEffect, ClusterWriteFailure, ClusterWriteObservation, ClusterWriteReceipt,
};
pub use options::{
    ClusterProtocol, ClusterProviderOptions, ClusterSource, ClusterStrategy, ClusterTransferLimits,
    ClusterUnixfsOptions,
};

use super::{ClusterNetworkPolicy, ClusterReplicaVerifier, ClusterResourceKey, ClusterRestProfile};
use crate::pinning::identity::Ownership;
use ClusterFailureCode as Code;
use ClusterWriteFailure as Failure;
use http::Http;

pub struct ClusterProvider {
    options: ClusterProviderOptions,
    verifier: ClusterReplicaVerifier,
    target: Http,
    source: Option<Http>,
}

impl ClusterProvider {
    pub fn new(options: ClusterProviderOptions) -> Result<Self, ClusterWriteFailure> {
        options.validate()?;
        let policy =
            ClusterNetworkPolicy::from_allow_private_network(options.allow_private_network);
        let profile = ClusterRestProfile::new_with_network_policy(
            options.pinset.clone(),
            Some(&options.rest_endpoint),
            options.rest_auth.clone(),
            options.min_replicas,
            options.verifier_limits,
            policy,
        )
        .map_err(|_| Failure::new(Code::InvalidConfiguration))?;
        let target_profile = match options.protocol {
            ClusterProtocol::Rest => profile.clone(),
            ClusterProtocol::Proxy => ClusterRestProfile::new_with_network_policy(
                options.pinset.clone(),
                options.proxy_endpoint.as_deref(),
                options.proxy_auth.clone(),
                options.min_replicas,
                options.verifier_limits,
                policy,
            )
            .map_err(|_| Failure::new(Code::InvalidConfiguration))?,
        };
        let target = Http::new(target_profile, options.transfer_limits)?;
        let source = options
            .source
            .as_ref()
            .map(|s| {
                let profile = ClusterRestProfile::new_with_network_policy(
                    options.pinset.clone(),
                    Some(&s.endpoint),
                    s.auth.clone(),
                    options.min_replicas,
                    options.verifier_limits,
                    policy,
                )
                .map_err(|_| Failure::new(Code::InvalidConfiguration))?;
                Http::new(profile, options.transfer_limits)
            })
            .transpose()?;
        let verifier = ClusterReplicaVerifier::new(profile)
            .map_err(|_| Failure::new(Code::InvalidConfiguration))?;
        Ok(Self {
            options,
            verifier,
            target,
            source,
        })
    }

    pub fn resource_key(&self, cid: &str) -> Result<ClusterResourceKey, Failure> {
        Ok(ClusterResourceKey {
            pinset: self.options.pinset.clone(),
            cid: canonical_cid(cid)?,
        })
    }

    /// Always fresh; 404/403/empty allocations are never an absence certificate.
    pub async fn observe(&self, cid: &str) -> super::ReplicaObservation {
        self.verifier.observe(cid).await
    }

    pub(super) fn record(
        &self,
        observation: &mut ClusterWriteObservation,
        cid: &str,
        status: ClusterResourceStatus,
    ) -> Result<usize, Failure> {
        let resource = self.resource_key(cid)?;
        if let Some(index) = observation
            .resources
            .iter()
            .position(|r| r.resource == resource)
        {
            observation.resources[index].status = status;
            return Ok(index);
        }
        observation.resources.push(ClusterObservedResource {
            resource,
            protocol: self.options.protocol,
            strategy: self.options.strategy,
            status,
            ownership: Ownership::Unknown,
            reported_allocations: None,
            observed_at: chrono::Utc::now(),
        });
        Ok(observation.resources.len() - 1)
    }
}

fn canonical_cid(value: &str) -> Result<String, Failure> {
    let cid = cid::Cid::try_from(value).map_err(|_| Failure::new(Code::InvalidCid))?;
    Ok(cid::Cid::new_v1(cid.codec(), *cid.hash()).to_string())
}
