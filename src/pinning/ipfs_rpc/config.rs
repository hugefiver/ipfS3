use std::{net::IpAddr, time::Duration};

use anyhow::{bail, ensure};
use serde::Deserialize;

use super::{RpcAuth, RpcProfile, RpcStrategy, RpcTimeouts};

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcProviderRegistry {
    #[serde(default)]
    pub providers: Vec<RpcProviderConfig>,
}

/// RPC options live separately to preserve the legacy ProviderConfig API.
/// Secrets are environment references only; no source-node credential is reused.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcProviderConfig {
    pub config_name: String,
    pub profile: String,
    #[serde(default = "no_auth")]
    pub auth: String,
    #[serde(default)]
    pub username_env: Option<String>,
    #[serde(default)]
    pub allow_private_network: bool,
    #[serde(default)]
    pub tls_ca_pem: Option<String>,
    #[serde(default)]
    pub tls_client_cert_pem: Option<String>,
    #[serde(default)]
    pub tls_client_key_pem: Option<String>,
    #[serde(default)]
    pub tls_insecure: bool,
    #[serde(default)]
    pub connect_timeout_seconds: Option<u64>,
    #[serde(default)]
    pub control_timeout_seconds: Option<u64>,
    #[serde(default)]
    pub idle_timeout_seconds: Option<u64>,
}

fn no_auth() -> String {
    "none".into()
}

#[derive(Debug, Clone)]
pub struct RpcProviderOptions {
    pub profile: RpcProfile,
    pub strategy: RpcStrategy,
    pub auth: Option<RpcAuth>,
    pub allow_private_network: bool,
    pub timeouts: RpcTimeouts,
}

impl RpcProviderConfig {
    pub(crate) fn validate<F>(
        &self,
        filebase: bool,
        strategy: Option<&str>,
        token: Option<&str>,
        get_env: &F,
    ) -> anyhow::Result<RpcProviderOptions>
    where
        F: Fn(&str) -> Option<String>,
    {
        let profile = match (filebase, self.profile.as_str()) {
            (true, "filebase") => RpcProfile::Filebase,
            (false, "kubo") => RpcProfile::Kubo,
            _ => bail!("RPC profile must match its provider kind (kubo or filebase)"),
        };
        ensure!(
            self.tls_ca_pem.is_none()
                && self.tls_client_cert_pem.is_none()
                && self.tls_client_key_pem.is_none()
                && !self.tls_insecure,
            "RPC custom TLS CA/mTLS/insecure options are not supported; certificate verification remains enabled"
        );
        let strategy = RpcStrategy::parse(strategy.ok_or_else(|| {
            anyhow::anyhow!("RPC strategy must explicitly be cid, upload or car")
        })?)
        .ok_or_else(|| anyhow::anyhow!("unknown RPC strategy"))?;
        profile.validate(strategy)?;
        let auth = match self.auth.as_str() {
            "none" if !filebase && token.is_none() && self.username_env.is_none() => None,
            "bearer" if self.username_env.is_none() => Some(RpcAuth::Bearer(
                token
                    .ok_or_else(|| anyhow::anyhow!("RPC bearer auth requires token_env"))?
                    .into(),
            )),
            "basic" if !filebase => {
                let username_env = self
                    .username_env
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("RPC basic auth requires username_env"))?;
                validate_env_reference(username_env)?;
                let username = get_env(username_env)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        anyhow::anyhow!("RPC basic username environment variable is not set")
                    })?;
                Some(RpcAuth::Basic {
                    username,
                    password: token
                        .ok_or_else(|| {
                            anyhow::anyhow!("RPC basic auth requires token_env for the password")
                        })?
                        .into(),
                })
            }
            _ => bail!("RPC auth must match the profile and configured credential references"),
        };
        if let Some(auth) = &auth {
            auth.header()?;
        }
        let defaults = RpcTimeouts::default();
        let duration = |value: Option<u64>, default: Duration| -> anyhow::Result<Duration> {
            let seconds = value.unwrap_or(default.as_secs());
            ensure!(
                (1..=86_400).contains(&seconds),
                "RPC timeouts must be between 1 and 86400 seconds"
            );
            Ok(Duration::from_secs(seconds))
        };
        Ok(RpcProviderOptions {
            profile,
            strategy,
            auth,
            allow_private_network: self.allow_private_network,
            timeouts: RpcTimeouts {
                connect: duration(self.connect_timeout_seconds, defaults.connect)?,
                control: duration(self.control_timeout_seconds, defaults.control)?,
                idle: duration(self.idle_timeout_seconds, defaults.idle)?,
            },
        })
    }
}

pub(crate) fn validate_env_reference(name: &str) -> anyhow::Result<()> {
    ensure!(
        !name.is_empty()
            && name
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'),
        "RPC credential environment reference must be uppercase canonical"
    );
    Ok(())
}

pub(crate) fn validate_endpoint(endpoint: &str, allow_private: bool) -> anyhow::Result<()> {
    let url = reqwest::Url::parse(endpoint)
        .map_err(|_| anyhow::anyhow!("invalid administrator RPC endpoint"))?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "administrator RPC endpoint must not contain URL credentials, query or fragment"
    );
    if !allow_private {
        ensure!(
            url.scheme() == "https",
            "public RPC endpoints require HTTPS"
        );
        match url.host().expect("host checked") {
            url::Host::Domain(host) => ensure!(
                host.contains('.')
                    && !host.ends_with('.')
                    && !host.ends_with(".localhost")
                    && !host.ends_with(".local")
                    && !host.ends_with(".internal"),
                "private RPC network access requires allow_private_network = true"
            ),
            url::Host::Ipv4(ip) => ensure!(
                public_ip(IpAddr::V4(ip)),
                "private RPC network access requires allow_private_network = true"
            ),
            url::Host::Ipv6(ip) => ensure!(
                public_ip(IpAddr::V6(ip)),
                "private RPC network access requires allow_private_network = true"
            ),
        }
    }
    Ok(())
}

pub(super) fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_multicast()
                || ip.is_unspecified()
                || ip.is_broadcast()
                || ip.is_documentation()
                || a == 0
                || a >= 240
                || (a == 100 && (64..=127).contains(&b))
                || (a == 198 && (18..=19).contains(&b))
                || (a == 192 && b == 0 && c == 0)
                || (a == 192 && b == 88 && c == 99))
        }
        IpAddr::V6(ip) => {
            if let Some(v4) = ip.to_ipv4_mapped() {
                return public_ip(IpAddr::V4(v4));
            }
            let segments = ip.segments();
            // Fail closed outside globally routed unicast, including transition
            // ranges that may tunnel to a private IPv4 destination.
            (segments[0] & 0xe000) == 0x2000
                && segments[0] != 0x2002
                && !(segments[0] == 0x2001 && segments[1] <= 0x01ff)
                && !(segments[0] == 0x2001 && segments[1] == 0x0db8)
        }
    }
}
