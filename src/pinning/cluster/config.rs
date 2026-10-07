//! Pure administrator configuration; no clients, DNS, DB or account probes.
#[path = "config/limits.rs"]
mod limits;

use anyhow::{anyhow, bail, ensure};
use serde::Deserialize;

use super::provider::{ClusterProtocol, ClusterProviderOptions, ClusterSource, ClusterStrategy};
use super::{ClusterAuth, ClusterNetworkPolicy, ClusterPinset, ClusterRestProfile};
use crate::pinning::{identity::ProviderIdentity, ipfs_rpc::validate_env_reference};
pub use limits::{ClusterTransferConfig, ClusterVerifierConfig};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterProviderConfig {
    pub protocol: String,
    pub rest_endpoint: String,
    #[serde(default)]
    pub proxy_endpoint: Option<String>,
    #[serde(default)]
    pub proxy_auth: ClusterCredentialConfig,
    #[serde(default)]
    pub rest_auth: ClusterCredentialConfig,
    /// Main Kubo bytes/DAG only. No alternate source endpoint is accepted.
    #[serde(default)]
    pub source: ClusterCredentialConfig,
    pub min_replicas: u32,
    #[serde(default)]
    pub allow_private_network: bool,
    #[serde(default)]
    pub transfer_limits: ClusterTransferConfig,
    #[serde(default)]
    pub verifier_limits: ClusterVerifierConfig,
    #[serde(default = "stream_channels")]
    pub stream_channels: bool,
}

fn stream_channels() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterCredentialConfig {
    #[serde(default = "no_auth")]
    pub auth: String,
    #[serde(default)]
    pub token_env: Option<String>,
    #[serde(default)]
    pub username_env: Option<String>,
}
fn no_auth() -> String {
    "none".into()
}
impl Default for ClusterCredentialConfig {
    fn default() -> Self {
        Self {
            auth: no_auth(),
            token_env: None,
            username_env: None,
        }
    }
}

impl ClusterCredentialConfig {
    pub(crate) fn secret_ref(&self) -> Option<String> {
        self.token_env.as_ref().map(|name| format!("env:{name}"))
    }

    fn resolve<F>(&self, get_env: &F) -> anyhow::Result<Option<ClusterAuth>>
    where
        F: Fn(&str) -> Option<String>,
    {
        let value = |reference: Option<&str>| -> anyhow::Result<String> {
            let reference =
                reference.ok_or_else(|| anyhow!("Cluster credential reference is required"))?;
            validate_env_reference(reference)?;
            get_env(reference)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| anyhow!("Cluster credential environment variable is not set"))
        };
        let auth = match self.auth.as_str() {
            "none" if self.token_env.is_none() && self.username_env.is_none() => None,
            "bearer" if self.username_env.is_none() => {
                Some(ClusterAuth::Bearer(value(self.token_env.as_deref())?))
            }
            "basic" => Some(ClusterAuth::Basic {
                username: value(self.username_env.as_deref())?,
                password: value(self.token_env.as_deref())?,
            }),
            _ => bail!("Cluster auth must match its independent credential references"),
        };
        if let Some(auth) = &auth {
            auth.header()?;
        }
        Ok(auth)
    }
}

impl ClusterProviderConfig {
    pub(crate) fn validate<F>(
        &self,
        strategy: Option<&str>,
        identity: &ProviderIdentity,
        primary: Option<&str>,
        get_env: &F,
    ) -> anyhow::Result<ClusterProviderOptions>
    where
        F: Fn(&str) -> Option<String>,
    {
        let protocol = match self.protocol.as_str() {
            "proxy" => ClusterProtocol::Proxy,
            "rest" => ClusterProtocol::Rest,
            _ => bail!("Cluster protocol must explicitly be proxy or rest"),
        };
        let strategy = match strategy {
            Some("cid") => ClusterStrategy::Cid,
            Some("upload") => ClusterStrategy::Upload,
            Some("car") => ClusterStrategy::Car,
            _ => bail!("Cluster strategy must explicitly be cid, upload or car"),
        };
        ensure!(
            protocol == ClusterProtocol::Proxy
                || (self.proxy_endpoint.is_none()
                    && self.proxy_auth.auth == "none"
                    && self.proxy_auth.token_env.is_none()
                    && self.proxy_auth.username_env.is_none()),
            "native Cluster REST must not configure Proxy options"
        );
        // The identity reference tracks the write credential. Endpoint/credential
        // revisions also bind independent verifier/source credentials in the ledger.
        let write_auth = match protocol {
            ClusterProtocol::Proxy => &self.proxy_auth,
            ClusterProtocol::Rest => &self.rest_auth,
        };
        ensure!(
            identity.secret_ref == write_auth.secret_ref(),
            "Cluster identity secret_ref does not match the write credential reference"
        );
        let policy = ClusterNetworkPolicy::from_allow_private_network(self.allow_private_network);
        if let Some(primary) = primary {
            validate_independence(
                primary,
                &self.rest_endpoint,
                self.proxy_endpoint.as_deref(),
                policy,
            )?;
        }
        let source = if strategy != ClusterStrategy::Cid {
            Some(ClusterSource {
                endpoint: primary
                    .ok_or_else(|| anyhow!("Cluster upload/CAR requires the main Kubo source"))?
                    .into(),
                auth: self.source.resolve(get_env)?,
            })
        } else {
            ensure!(
                self.source.auth == "none"
                    && self.source.token_env.is_none()
                    && self.source.username_env.is_none(),
                "Cluster CID route does not use source credentials"
            );
            None
        };
        let options = ClusterProviderOptions {
            protocol,
            strategy,
            proxy_endpoint: self.proxy_endpoint.clone(),
            rest_endpoint: self.rest_endpoint.clone(),
            proxy_auth: self.proxy_auth.resolve(get_env)?,
            rest_auth: self.rest_auth.resolve(get_env)?,
            source,
            pinset: ClusterPinset::new(&identity.backend, &identity.scope)?,
            min_replicas: self.min_replicas as usize,
            allow_private_network: self.allow_private_network,
            transfer_limits: self.transfer_limits.validate()?,
            verifier_limits: self.verifier_limits.validate()?,
            stream_channels: self.stream_channels,
            unixfs: None,
        };
        options.validate()?;
        ClusterRestProfile::new_with_network_policy(
            options.pinset.clone(),
            Some(&options.rest_endpoint),
            options.rest_auth.clone(),
            options.min_replicas,
            options.verifier_limits,
            policy,
        )?;
        Ok(options)
    }
}

pub(crate) fn normalized_endpoint(value: &str) -> anyhow::Result<reqwest::Url> {
    let mut url =
        reqwest::Url::parse(value).map_err(|_| anyhow!("invalid Cluster source endpoint"))?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "invalid Cluster source endpoint"
    );
    let path = url
        .path()
        .trim_end_matches('/')
        .trim_end_matches("/api/v0")
        .to_owned();
    url.set_path(&path);
    Ok(url)
}

pub(crate) fn validate_independence(
    primary: &str,
    rest: &str,
    proxy: Option<&str>,
    policy: ClusterNetworkPolicy,
) -> anyhow::Result<()> {
    let primary = normalized_endpoint(primary)?;
    for target in [Some(rest), proxy].into_iter().flatten() {
        super::network::endpoint(target, policy)
            .map_err(|_| anyhow!("invalid administrator Cluster endpoint"))?;
        ensure!(
            normalized_endpoint(target)? != primary,
            "Cluster target must be independent of main Kubo"
        );
    }
    Ok(())
}
