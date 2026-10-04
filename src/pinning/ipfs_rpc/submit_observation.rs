use super::{IpfsRpcProvider, RpcProfile, RpcStrategy, cid, error};
use crate::pinning::{
    identity::{Ownership, RemoteResourceType},
    provider::{ProviderError, ProviderErrorClass, RemotePin, SubmitPin},
};
use serde::{Deserialize, Serialize};

pub const MAX_OBSERVED_RESOURCES: usize = 16;

/// Finite evidence states, not guesses about resource creation or ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcResourceStatus {
    /// A valid root record was received, but clean whole-response completion is
    /// not established. Neither stored-copy nor pin acknowledgment is claimed.
    Reported,
    /// Kubo add accepted the root with pin=false; no pin claim.
    Stored,
    /// Clean add-and-pin/pin/add/import response acknowledged this root.
    /// This is NOT recursive/local verification and NOT a Pinned result.
    PinAccepted,
    /// Import reported a root PinErrorMsg. Its pin state remains uncertain;
    /// a preexisting external pin may still exist.
    PinError,
    /// Independent recursive observation passed (plus offline/local DAG and
    /// stable node checks for Kubo). Ownership is still Unknown.
    RecursiveVerified,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcObservedResource {
    pub resource_type: RemoteResourceType,
    pub cid: String,
    pub request_id: String,
    pub status: RpcResourceStatus,
    pub ownership: Ownership,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcSubmitEffect {
    NotSubmitted,
    /// Clean response roots were recorded. Does not prove application creation,
    /// exclusivity, or absence of other preexisting resources.
    Observed,
    /// An in-flight write, incomplete/invalid response or observation bound
    /// leaves additional effects unknown. Known prior resources remain listed.
    Unknown,
}

#[derive(Debug)]
pub struct RpcSubmitObservation {
    pub result: Result<RemotePin, ProviderError>,
    /// At most MAX_OBSERVED_RESOURCES canonical, deduplicated resources.
    pub resources: Vec<RpcObservedResource>,
    pub effect: RpcSubmitEffect,
}

impl From<RpcSubmitObservation> for crate::pinning::provider::SubmitObservation {
    fn from(observation: RpcSubmitObservation) -> Self {
        use crate::pinning::provider::{ObservedResource, ObservedResourceStatus, SubmitEffect};
        Self {
            result: observation.result,
            resources: observation
                .resources
                .into_iter()
                .map(|resource| ObservedResource {
                    resource_type: resource.resource_type,
                    cid: resource.cid,
                    request_id: resource.request_id,
                    status: match resource.status {
                        RpcResourceStatus::Reported => ObservedResourceStatus::Reported,
                        RpcResourceStatus::Stored => ObservedResourceStatus::Stored,
                        RpcResourceStatus::PinAccepted => ObservedResourceStatus::PinAccepted,
                        RpcResourceStatus::PinError => ObservedResourceStatus::PinError,
                        RpcResourceStatus::RecursiveVerified => {
                            ObservedResourceStatus::RecursiveVerified
                        }
                    },
                    ownership: resource.ownership,
                })
                .collect(),
            effect: match observation.effect {
                RpcSubmitEffect::NotSubmitted => SubmitEffect::NotSubmitted,
                RpcSubmitEffect::Observed => SubmitEffect::Observed,
                RpcSubmitEffect::Unknown => SubmitEffect::Unknown,
            },
        }
    }
}

pub(super) struct SubmitEvidence {
    resources: Vec<RpcObservedResource>,
    effect: RpcSubmitEffect,
}

impl SubmitEvidence {
    fn new() -> Self {
        Self {
            resources: Vec::new(),
            effect: RpcSubmitEffect::NotSubmitted,
        }
    }
    pub fn begin_write(&mut self) {
        self.effect = RpcSubmitEffect::Unknown;
    }
    pub fn unknown(&mut self) {
        self.effect = RpcSubmitEffect::Unknown;
    }

    pub fn reported(&mut self, profile: RpcProfile, cid: String) -> Result<(), ProviderError> {
        self.unknown();
        self.record(profile, cid, RpcResourceStatus::Reported)
    }

    fn record(
        &mut self,
        profile: RpcProfile,
        reported: String,
        status: RpcResourceStatus,
    ) -> Result<(), ProviderError> {
        let cid = cid::canonical(&reported)
            .map_err(|_| error::protocol("RPC resource observation contains an invalid CID"))?;
        if let Some(existing) = self.resources.iter_mut().find(|r| r.cid == cid) {
            use RpcResourceStatus::*;
            if matches!(
                (existing.status, status),
                (Reported, _)
                    | (Stored, PinAccepted | PinError | RecursiveVerified)
                    | (PinAccepted, PinError | RecursiveVerified)
                    | (PinError, RecursiveVerified)
            ) {
                existing.status = status;
            }
            return Ok(());
        }
        if self.resources.len() == MAX_OBSERVED_RESOURCES {
            return Err(error::protocol(
                "RPC resource observation exceeds its bound",
            ));
        }
        let request_id = cid::request_id(profile, &cid)?;
        self.resources.push(RpcObservedResource {
            resource_type: RemoteResourceType::RpcPin,
            cid,
            request_id,
            status,
            ownership: Ownership::Unknown,
        });
        Ok(())
    }

    /// Called only after clean HTTP 200 EOF/trailer validation. Keep known valid
    /// roots even if another root is invalid or the bounded ledger list fills.
    pub fn clean(
        &mut self,
        profile: RpcProfile,
        roots: impl IntoIterator<Item = (String, RpcResourceStatus)>,
    ) -> Result<(), ProviderError> {
        let mut incomplete = false;
        let mut seen = false;
        for (reported, status) in roots {
            seen = true;
            if self.record(profile, reported, status).is_err() {
                incomplete = true;
            }
        }
        if incomplete || !seen {
            self.unknown();
            return Err(error::protocol(
                "RPC resource observation is incomplete or exceeds its bound",
            ));
        }
        self.effect = RpcSubmitEffect::Observed;
        Ok(())
    }

    fn finish(mut self, mut result: Result<RemotePin, ProviderError>) -> RpcSubmitObservation {
        if let Err(error) = &mut result
            && error.definitely_not_submitted()
        {
            // Only the local preflight/preconnection sentinel can establish
            // non-submission after begin_write. HTTP rejection classes are
            // not effect evidence for a dispatched RPC/proxy write.
            if error.class == ProviderErrorClass::NotSubmitted && self.resources.is_empty() {
                self.effect = RpcSubmitEffect::NotSubmitted;
            } else if self.effect != RpcSubmitEffect::NotSubmitted {
                // A later preconnection/rejection cannot erase an earlier
                // acknowledged write or advertise overall not-created.
                error.class = ProviderErrorClass::Protocol;
            }
        }
        RpcSubmitObservation {
            result,
            resources: self.resources,
            effect: self.effect,
        }
    }
}

impl IpfsRpcProvider {
    pub async fn submit_observed(&self, request: SubmitPin) -> RpcSubmitObservation {
        let mut evidence = SubmitEvidence::new();
        let result = self.submit_recorded(request, &mut evidence).await;
        evidence.finish(result)
    }

    async fn submit_recorded(
        &self,
        request: SubmitPin,
        evidence: &mut SubmitEvidence,
    ) -> Result<RemotePin, ProviderError> {
        cid::parse(&request.cid)?;
        match self.strategy {
            RpcStrategy::Cid => self.pin_cid(&request.cid, evidence).await?,
            RpcStrategy::Upload => self.upload(&request.cid, evidence).await?,
            RpcStrategy::Car => self.import_car(&request.cid, evidence).await?,
        }
        let pins = self.find_verified(&request.cid).await.map_err(|_| {
            error::protocol("RPC submit lacks authoritative recursive copy verification")
        })?;
        let pin = pins
            .into_iter()
            .next()
            .ok_or_else(|| error::protocol("RPC submit returned no recursive pin"))?;
        evidence.clean(
            self.profile,
            [(pin.cid.clone(), RpcResourceStatus::RecursiveVerified)],
        )?;
        Ok(pin)
    }
}

#[cfg(test)]
mod evidence_regressions {
    use super::*;

    #[test]
    fn dispatched_rejection_cannot_erase_unknown_effect() {
        for class in [
            ProviderErrorClass::Authentication,
            ProviderErrorClass::UnknownForbidden,
            ProviderErrorClass::InvalidInput,
            ProviderErrorClass::RateLimited,
        ] {
            let mut evidence = SubmitEvidence::new();
            evidence.begin_write();
            let observation = evidence.finish(Err(error::error(class, "postdispatch rejection")));
            assert_eq!(observation.effect, RpcSubmitEffect::Unknown, "{class:?}");
        }
    }
}
