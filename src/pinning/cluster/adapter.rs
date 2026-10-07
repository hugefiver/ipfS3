//! Trait boundary. Serialized facts never become live replica proof.
#[path = "adapter/factory.rs"]
mod factory;
#[path = "adapter/observation.rs"]
mod observation;
pub use factory::build_cluster_provider;

use super::{
    ReplicaOutcome, VerificationCode,
    provider::{ClusterProtocol, ClusterProvider, ClusterStrategy},
};
use crate::pinning::provider::{
    FindPin, PinObservation, PinningProvider, ProviderError, ProviderErrorClass, RemotePin,
    SubmitObservation, SubmitPin,
};

pub(super) struct ClusterAdapter {
    name: String,
    protocol: ClusterProtocol,
    strategy: ClusterStrategy,
    leaf: ClusterProvider,
}

fn unavailable() -> ProviderError {
    ProviderError {
        class: ProviderErrorClass::Protocol,
        message: "Cluster route or fresh replica confirmation unavailable; needs_attention".into(),
        retry_after: None,
    }
}

#[async_trait::async_trait]
impl PinningProvider for ClusterAdapter {
    fn name(&self) -> &str {
        &self.name
    }
    fn invocation_route(&self) -> (&'static str, &'static str) {
        (self.protocol.api_profile(), self.strategy.as_str())
    }
    async fn submit(&self, request: SubmitPin) -> Result<RemotePin, ProviderError> {
        self.submit_observed(request).await.result
    }
    async fn submit_observed(&self, request: SubmitPin) -> SubmitObservation {
        observation::submit(self.leaf.submit_observed(request).await)
    }
    async fn get(&self, id: &str) -> Result<RemotePin, ProviderError> {
        self.get_observed(id).await.result
    }
    async fn get_observed(&self, id: &str) -> PinObservation {
        let resource = match self.leaf.resource_key(id) {
            Ok(resource) => resource,
            Err(error) => {
                return PinObservation {
                    result: Err(observation::write_error(error)),
                    resources: vec![],
                };
            }
        };
        let proof = self.leaf.observe(&resource.cid).await;
        // Bind to requested resource AND consume the live outcome immediately.
        let bound = proof.evidence().is_some_and(|e| e.resource == resource);
        let evidence = bound.then(|| proof.evidence().cloned()).flatten();
        let outcome = if bound {
            proof.outcome()
        } else {
            ReplicaOutcome::Unknown
        };
        let observed = observation::resource(&resource.cid, outcome, evidence, false);
        PinObservation {
            result: match outcome {
                ReplicaOutcome::Unknown => Err(proof
                    .failure()
                    .filter(|failure| failure.code == VerificationCode::Authentication)
                    .map(|failure| ProviderError {
                        class: ProviderErrorClass::Authentication,
                        // VerificationFailure contains only enums/numeric status,
                        // never response bodies, credentials or endpoint details.
                        message: failure.to_string(),
                        retry_after: None,
                    })
                    .unwrap_or_else(unavailable)),
                _ => Ok(observation::pin(&resource.cid, outcome)),
            },
            resources: vec![observed],
        }
    }
    async fn get_observed_historical(&self, id: &str, api: &str, strategy: &str) -> PinObservation {
        if self.invocation_route() != (api, strategy) {
            return PinObservation {
                result: Err(unavailable()),
                resources: vec![],
            };
        }
        self.get_observed(id).await
    }
    async fn find(&self, query: FindPin) -> Result<Vec<RemotePin>, ProviderError> {
        // There is no authoritative metadata lookup/absence certificate in this
        // leaf. Never claim an empty result or reuse a pin under another job.
        if !query.metadata.is_empty() {
            return Err(unavailable());
        }
        Ok(vec![self.get(&query.cid).await?])
    }
    async fn unpin(&self, _id: &str) -> Result<(), ProviderError> {
        // Every transport footprint has Unknown ownership. An untyped CID is
        // not the authorized historical reference required by the remove leaf.
        Err(unavailable())
    }
}
