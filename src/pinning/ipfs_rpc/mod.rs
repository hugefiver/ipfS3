//! RPC pin transport for a backup Kubo or Filebase node.
mod add;
mod body;
mod car;
mod cid;
mod config;
mod error;
mod profile;
mod provider;
mod streaming;
mod transport;

pub use config::{RpcProviderConfig, RpcProviderOptions, RpcProviderRegistry};
pub(crate) use config::{validate_endpoint, validate_env_reference};
pub use profile::{RpcAuth, RpcProfile, RpcStrategy};
pub use transport::RpcTimeouts;

use crate::{
    kubo::KuboClient,
    pinning::provider::{ProviderError, ProviderErrorClass},
};
use transport::Transport;

pub(crate) fn same_endpoint(left: &str, right: &str) -> bool {
    let (Ok(left), Ok(right)) = (reqwest::Url::parse(left), reqwest::Url::parse(right)) else {
        return false;
    };
    fn base_path(url: &reqwest::Url) -> &str {
        url.path().trim_end_matches('/').trim_end_matches("/api/v0")
    }
    left.origin() == right.origin() && base_path(&left) == base_path(&right)
}

#[cfg(test)]
mod add_grammar_tests;
#[cfg(test)]
mod additional_tests;
#[cfg(test)]
mod stored_bytes_tests;
#[cfg(test)]
mod stream_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod transport_tests;

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
        if source
            .as_ref()
            .is_some_and(|source| same_endpoint(&endpoint, source.base_url()))
        {
            return Err(error::error(
                ProviderErrorClass::InvalidInput,
                "RPC target must not be the primary Kubo endpoint",
            ));
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
}

fn not_submitted(message: &'static str) -> ProviderError {
    error::error(ProviderErrorClass::NotSubmitted, message)
}
