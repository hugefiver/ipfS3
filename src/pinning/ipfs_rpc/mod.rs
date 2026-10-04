//! Leaf RPC provider implementation. Registration/configuration and ledger
//! authorization belong to the parent. No operation here grants pin ownership.
mod add;
mod body;
mod car;
mod cid;
mod config;
mod error;
mod observe;
mod profile;
mod provider;
mod streaming;
mod submit_observation;
mod transport;

pub use config::{RpcProviderConfig, RpcProviderOptions, RpcProviderRegistry};
pub(crate) use config::{validate_endpoint, validate_env_reference};
pub use observe::{RpcPinKind, RpcPinObservation};
pub use profile::{RpcAuth, RpcProfile, RpcStrategy};
pub use submit_observation::{
    MAX_OBSERVED_RESOURCES, RpcObservedResource, RpcResourceStatus, RpcSubmitEffect,
    RpcSubmitObservation,
};
pub use transport::RpcTimeouts;

use crate::{
    kubo::KuboClient,
    pinning::{
        identity::{Ownership, ProviderRouteSnapshot, RemoteResourceType},
        provider::{
            FindPin, ProviderError, ProviderErrorClass, QueryObservation, RemotePin,
            RemotePinStatus, RemoteRef,
        },
    },
};
use serde::Deserialize;
use transport::Transport;

#[cfg(test)]
mod add_grammar_tests;
#[cfg(test)]
mod additional_tests;
#[cfg(test)]
mod observation_tests;
#[cfg(test)]
mod stored_bytes_tests;
#[cfg(test)]
mod stream_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod transport_tests;
#[cfg(test)]
mod typed_trait_tests;

#[derive(Clone)]
pub struct IpfsRpcProvider {
    name: String,
    profile: RpcProfile,
    strategy: RpcStrategy,
    target: Transport,
    source: Option<Transport>,
}

impl IpfsRpcProvider {
    /// `endpoint` must be an administrator-trusted RPC base, not a tag or object
    /// value. Accepts origin/prefix or an already suffixed /api/v0 base. Private
    /// network permission and TLS/CA/mTLS policy remain registration concerns;
    /// the default clients retain certificate verification and forbid redirects.
    pub fn new(
        name: String,
        endpoint: String,
        source: KuboClient,
        profile: RpcProfile,
        strategy: RpcStrategy,
        auth: Option<RpcAuth>,
    ) -> Result<Self, ProviderError> {
        Self::new_with_timeouts(
            name,
            endpoint,
            source,
            profile,
            strategy,
            auth,
            RpcTimeouts::default(),
        )
    }

    pub fn new_with_timeouts(
        name: String,
        endpoint: String,
        source: KuboClient,
        profile: RpcProfile,
        strategy: RpcStrategy,
        auth: Option<RpcAuth>,
        timeouts: RpcTimeouts,
    ) -> Result<Self, ProviderError> {
        Self::from_config(
            name,
            endpoint,
            Some(source),
            &RpcProviderOptions {
                profile,
                strategy,
                auth,
                allow_private_network: true,
                timeouts,
            },
        )
    }

    /// Registration validates explicit identity and supplies a separate source.
    /// CID strategy needs no source client; upload/CAR require stored-byte access.
    pub(crate) fn from_config(
        name: String,
        endpoint: String,
        source: Option<KuboClient>,
        options: &RpcProviderOptions,
    ) -> Result<Self, ProviderError> {
        let RpcProviderOptions {
            profile,
            strategy,
            auth,
            timeouts,
            allow_private_network,
        } = options;
        profile.validate(*strategy)?;
        if name.trim().is_empty() {
            return Err(error::error(
                ProviderErrorClass::InvalidInput,
                "RPC provider name must not be empty",
            ));
        }
        if *profile == RpcProfile::Filebase && !matches!(&auth, Some(RpcAuth::Bearer(_))) {
            return Err(error::error(
                ProviderErrorClass::InvalidInput,
                "Filebase RPC requires a bucket-scoped bearer token",
            ));
        }
        if *strategy != RpcStrategy::Cid && source.is_none() {
            return Err(error::error(
                ProviderErrorClass::InvalidInput,
                "RPC upload/CAR requires a separate primary Kubo source",
            ));
        }
        if let Some(source) = &source {
            let normalized = |value: &str| -> Option<reqwest::Url> {
                let mut url = reqwest::Url::parse(value).ok()?;
                let prefix = url
                    .path()
                    .trim_end_matches('/')
                    .trim_end_matches("/api/v0")
                    .to_owned();
                url.set_path(&prefix);
                Some(url)
            };
            if normalized(&endpoint).is_some()
                && normalized(&endpoint) == normalized(source.base_url())
            {
                return Err(error::error(
                    ProviderErrorClass::InvalidInput,
                    "RPC target must not be the primary Kubo endpoint",
                ));
            }
        }
        Ok(Self {
            name,
            profile: *profile,
            strategy: *strategy,
            target: Transport::with_network_policy(
                &endpoint,
                auth.as_ref(),
                *timeouts,
                *allow_private_network,
            )?,
            // A narrow source adapter deliberately ignores local-only read
            // policy for complete CAR export and never inherits target auth.
            source: source
                .map(|source| Transport::new(source.base_url(), None, *timeouts))
                .transpose()?,
        })
    }

    pub async fn observe(&self, query: FindPin) -> QueryObservation {
        match self.find_verified(&query.cid).await {
            Ok(pins) => QueryObservation::Complete(pins),
            Err(error) => QueryObservation::Unknown(error),
        }
    }

    /// Build an unknown-ownership reference for parent ledger publication. A
    /// successful submit/get/find is NOT proof of ApplicationCreated ownership.
    pub fn remote_ref(
        &self,
        pin: &RemotePin,
        route: ProviderRouteSnapshot,
    ) -> Result<RemoteRef, ProviderError> {
        let cid = cid::decode(self.profile, &pin.request_id)?;
        let profile_matches = match route.api_profile.as_str() {
            "rpc" => true,
            "kubo" => self.profile == RpcProfile::Kubo,
            "filebase-rpc" => self.profile == RpcProfile::Filebase,
            _ => false,
        };
        let strategy_matches = RpcStrategy::parse(&route.strategy)
            .is_some_and(|strategy| self.profile.validate(strategy).is_ok());
        if !cid::equivalent(&cid, &pin.cid).unwrap_or(false)
            || route.resource_type() != RemoteResourceType::RpcPin
            || !profile_matches
            || !strategy_matches
        {
            return Err(error::protocol(
                "RPC pin reference does not match its route or CID",
            ));
        }
        Ok(RemoteRef {
            resource_type: RemoteResourceType::RpcPin,
            cid,
            opaque_id: pin.request_id.clone(),
            route,
            ownership: Ownership::Unknown,
        })
    }

    fn historical_route(&self, api: &str, strategy: &str) -> Result<(), ProviderError> {
        let strategy = RpcStrategy::parse(strategy)
            .ok_or_else(|| error::protocol("historical RPC route unavailable"))?;
        if (api != "rpc" && api != self.profile.api_profile())
            || self.profile.validate(strategy).is_err()
        {
            return Err(error::protocol("historical RPC route unavailable"));
        }
        Ok(())
    }

    fn remote_pin(&self, requested: &str) -> Result<RemotePin, ProviderError> {
        Ok(RemotePin {
            request_id: cid::request_id(self.profile, requested)?,
            cid: cid::canonical(requested)?,
            status: RemotePinStatus::Pinned,
            raw_status: "recursive".into(),
            failure_reason: None,
        })
    }

    async fn pin_cid(
        &self,
        requested: &str,
        evidence: &mut submit_observation::SubmitEvidence,
    ) -> Result<(), ProviderError> {
        let requested = cid::canonical(requested)?;
        evidence.begin_write();
        let response = self
            .target
            .control(
                "pin/add",
                &[
                    ("arg", &requested),
                    ("recursive", "true"),
                    ("progress", "false"),
                ],
                true,
            )
            .await?;
        response.check(true)?;
        let pins: Pins = response.json()?;
        evidence.clean(
            self.profile,
            pins.pins
                .iter()
                .cloned()
                .map(|cid| (cid, RpcResourceStatus::PinAccepted)),
        )?;
        if pins.pins.len() != 1 {
            return Err(error::protocol(
                "RPC pin/add did not confirm the exact recursive root",
            ));
        }
        if !cid::equivalent(&pins.pins[0], &requested).unwrap_or(false) {
            return Err(error::mismatch());
        }
        Ok(())
    }
}

#[derive(Deserialize)]
pub(super) struct Pins {
    #[serde(rename = "Pins")]
    pub pins: Vec<String>,
}

fn not_submitted(message: &'static str) -> ProviderError {
    error::error(ProviderErrorClass::NotSubmitted, message)
}
