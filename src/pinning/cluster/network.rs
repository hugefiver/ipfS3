//! One connection-bound policy for source, write and read-only verification.
use reqwest::{
    Client, ClientBuilder, Url,
    dns::{Addrs, Name, Resolve, Resolving},
};
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

/// Private destinations and cleartext HTTP require explicit administrator opt-in.
/// TLS certificate and hostname validation remain enabled in both modes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ClusterNetworkPolicy {
    #[default]
    PublicOnly,
    AllowPrivate,
}

impl ClusterNetworkPolicy {
    pub const fn from_allow_private_network(allow: bool) -> Self {
        if allow {
            Self::AllowPrivate
        } else {
            Self::PublicOnly
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum NetworkFailure {
    #[error("invalid Cluster endpoint")]
    InvalidEndpoint,
    #[error("Cluster endpoint denied by network policy")]
    DeniedEndpoint,
    #[error("Cluster DNS answer denied by network policy")]
    DeniedResolution,
    #[error("Cluster DNS deadline exceeded")]
    ResolutionTimeout,
    #[error("Cluster DNS lookup failed")]
    ResolutionFailure,
}

pub(crate) fn endpoint(value: &str, policy: ClusterNetworkPolicy) -> Result<Url, NetworkFailure> {
    // URL normalization erases empty userinfo (`https://@host`). Inspect the
    // original absolute authority too, before normalization loses that intent.
    let (_, rest) = value
        .split_once("://")
        .ok_or(NetworkFailure::InvalidEndpoint)?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    if authority.is_empty()
        || authority.contains(['@', '\\'])
        || value.bytes().any(|b| b.is_ascii_control() || b == b' ')
    {
        return Err(NetworkFailure::InvalidEndpoint);
    }
    let url = Url::parse(value).map_err(|_| NetworkFailure::InvalidEndpoint)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(NetworkFailure::InvalidEndpoint);
    }
    if policy == ClusterNetworkPolicy::PublicOnly {
        let allowed = match url.host().expect("host validated") {
            url::Host::Ipv4(ip) => public_ip(ip.into()),
            url::Host::Ipv6(ip) => public_ip(ip.into()),
            url::Host::Domain(host) => {
                let host = host.strip_suffix('.').unwrap_or(host);
                host.contains('.')
                    && host.len() <= 253
                    && host.split('.').all(|label| {
                        !label.is_empty()
                            && label.len() <= 63
                            && !label.starts_with('-')
                            && !label.ends_with('-')
                            && label
                                .bytes()
                                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                    })
                    && ![".localhost", ".local", ".internal"]
                        .iter()
                        .any(|s| host.ends_with(s))
            }
        };
        if url.scheme() != "https" || !allowed {
            return Err(NetworkFailure::DeniedEndpoint);
        }
    }
    Ok(url)
}

pub(crate) fn public_ip(ip: IpAddr) -> bool {
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
                return public_ip(v4.into());
            }
            let s = ip.segments();
            (s[0] & 0xe000) == 0x2000
                && s[0] != 0x2002
                && !(s[0] == 0x2001 && (s[1] <= 0x1ff || s[1] == 0xdb8))
        }
    }
}

const MAX_ADDRESSES: usize = 64;

pub(crate) fn addresses(
    policy: ClusterNetworkPolicy,
    values: Vec<SocketAddr>,
) -> Result<Addrs, NetworkFailure> {
    // Reject the ENTIRE answer, not just the private members of a mixed answer.
    // Port zero binds the allowed IPs to the original URL port, never DNS input.
    if values.is_empty()
        || values.len() > MAX_ADDRESSES
        || (policy == ClusterNetworkPolicy::PublicOnly && values.iter().any(|a| !public_ip(a.ip())))
    {
        return Err(NetworkFailure::DeniedResolution);
    }
    Ok(Box::new(
        values.into_iter().map(|a| SocketAddr::new(a.ip(), 0)),
    ))
}

struct PolicyResolver {
    policy: ClusterNetworkPolicy,
    deadline: Duration,
    #[cfg(test)]
    inner: Option<Arc<dyn Resolve>>,
}

impl Resolve for PolicyResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let policy = self.policy;
        let deadline = self.deadline;
        #[cfg(test)]
        let inner = self.inner.clone();
        Box::pin(async move {
            let lookup = async {
                #[cfg(test)]
                if let Some(inner) = inner {
                    return inner
                        .resolve(name)
                        .await
                        .map(|a| a.take(MAX_ADDRESSES + 1).collect::<Vec<_>>())
                        .map_err(|_| NetworkFailure::ResolutionFailure);
                }
                tokio::net::lookup_host((name.as_str(), 0))
                    .await
                    .map(|a| a.take(MAX_ADDRESSES + 1).collect::<Vec<_>>())
                    .map_err(|_| NetworkFailure::ResolutionFailure)
            };
            let result = tokio::time::timeout(deadline, lookup)
                .await
                .map_err(|_| NetworkFailure::ResolutionTimeout)?;
            addresses(policy, result?)
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        })
    }
}

fn builder(resolver: PolicyResolver, connect: Duration, total: Option<Duration>) -> ClientBuilder {
    let mut builder = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        // No pooled or multiplexed connection may skip a subsequent resolution.
        .http1_only()
        .pool_max_idle_per_host(0)
        .connect_timeout(connect)
        .dns_resolver(Arc::new(resolver));
    if let Some(total) = total {
        builder = builder.timeout(total);
    }
    // Original URL/hostname reaches the connector and TLS verifier unchanged.
    builder
}

pub(crate) fn client(
    base: &Url,
    policy: ClusterNetworkPolicy,
    connect: Duration,
    total: Option<Duration>,
) -> Result<Client, NetworkFailure> {
    endpoint(base.as_str(), policy)?;
    builder(
        PolicyResolver {
            policy,
            deadline: connect,
            #[cfg(test)]
            inner: None,
        },
        connect,
        total,
    )
    .build()
    .map_err(|_| NetworkFailure::InvalidEndpoint)
}

/// Test DNS injection still goes THROUGH policy/deadline checks; never overrides
/// reqwest's address map or TLS verifier. Not present in production builds.
#[cfg(test)]
pub(crate) fn client_with_resolver(
    base: &Url,
    policy: ClusterNetworkPolicy,
    connect: Duration,
    total: Option<Duration>,
    resolver: Arc<dyn Resolve>,
) -> Result<Client, NetworkFailure> {
    endpoint(base.as_str(), policy)?;
    builder(
        PolicyResolver {
            policy,
            deadline: connect,
            inner: Some(resolver),
        },
        connect,
        total,
    )
    .build()
    .map_err(|_| NetworkFailure::InvalidEndpoint)
}

#[cfg(test)]
pub(crate) fn client_with_test_root(
    base: &Url,
    policy: ClusterNetworkPolicy,
    connect: Duration,
    resolver: Arc<dyn Resolve>,
    root: reqwest::Certificate,
) -> Result<Client, NetworkFailure> {
    endpoint(base.as_str(), policy)?;
    builder(
        PolicyResolver {
            policy,
            deadline: connect,
            inner: Some(resolver),
        },
        connect,
        Some(Duration::from_secs(2)),
    )
    .tls_certs_merge([root])
    .build()
    .map_err(|_| NetworkFailure::InvalidEndpoint)
}
