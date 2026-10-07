use super::{ClusterNetworkPolicy, RestOperation, VerificationCode, VerificationFailure, network};
use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::{Url, header::HeaderValue};
use std::{fmt, time::Duration};

fn invalid() -> VerificationFailure {
    VerificationFailure::new(
        RestOperation::Configure,
        VerificationCode::InvalidConfiguration,
    )
}

/// Administrator-declared pinset identity, independent of endpoint and credentials.
/// This identifies a logical pinset, NOT a physical failure domain.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterPinset {
    pub backend: String,
    pub scope: String,
}

impl ClusterPinset {
    pub fn new(backend: &str, scope: &str) -> Result<Self, VerificationFailure> {
        if [backend, scope].iter().any(|v| {
            v.is_empty()
                || v.len() > 128
                || !v
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        }) {
            return Err(invalid());
        }
        Ok(Self {
            backend: backend.into(),
            scope: scope.into(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterResourceKey {
    pub pinset: ClusterPinset,
    /// Canonical CIDv1, preserving codec and multihash (CIDv0 aliases collapse).
    pub cid: String,
}

#[derive(Clone, PartialEq, Eq)]
pub enum ClusterAuth {
    Bearer(String),
    Basic { username: String, password: String },
}

impl fmt::Debug for ClusterAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClusterAuth([REDACTED])")
    }
}

impl ClusterAuth {
    pub(crate) fn header(&self) -> Result<HeaderValue, VerificationFailure> {
        let text = match self {
            Self::Bearer(token)
                if !token.is_empty()
                    && token.len() <= 8192
                    && token.bytes().all(|b| b.is_ascii_graphic()) =>
            {
                format!("Bearer {token}")
            }
            Self::Basic { username, password }
                if !username.contains(':')
                    && !username.contains(['\r', '\n'])
                    && !password.contains(['\r', '\n'])
                    && username.len() + password.len() <= 8192 =>
            {
                format!(
                    "Basic {}",
                    STANDARD.encode(format!("{username}:{password}"))
                )
            }
            _ => return Err(invalid()),
        };
        let mut value = HeaderValue::from_str(&text).map_err(|_| invalid())?;
        value.set_sensitive(true);
        Ok(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestLimits {
    pub connect_timeout: Duration,
    /// One total deadline for version + BOTH allocations + global status, not per call.
    pub total_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_body_bytes: usize,
    /// Age of this local observation window; NOT the remote pin creation timestamp.
    pub max_observation_age: Duration,
}

impl Default for RestLimits {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(3),
            total_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(3),
            max_body_bytes: 256 * 1024,
            max_observation_age: Duration::from_secs(30),
        }
    }
}

impl RestLimits {
    fn validate(self) -> Result<(), VerificationFailure> {
        let ceiling = Duration::from_secs(300);
        if [
            self.connect_timeout,
            self.total_timeout,
            self.idle_timeout,
            self.max_observation_age,
        ]
        .iter()
        .any(|d| d.is_zero() || *d > ceiling)
            || self.connect_timeout > self.total_timeout
            || self.idle_timeout > self.total_timeout
            || self.max_body_bytes == 0
            || self.max_body_bytes > 4 * 1024 * 1024
        {
            return Err(invalid());
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ClusterRestProfile {
    pub(super) pinset: ClusterPinset,
    pub(super) base: Url,
    pub(super) authorization: Option<HeaderValue>,
    pub(super) min_replicas: usize,
    pub(super) limits: RestLimits,
    pub(super) network_policy: ClusterNetworkPolicy,
}

impl fmt::Debug for ClusterRestProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterRestProfile")
            .field("pinset", &self.pinset)
            .field("min_replicas", &self.min_replicas)
            .field("limits", &self.limits)
            .field("network_policy", &self.network_policy)
            .finish_non_exhaustive()
    }
}

impl ClusterRestProfile {
    /// Only trusted administrator REST endpoints; no Proxy fallback or discovery.
    /// Signature-compatible constructor, with secure public-only defaults.
    pub fn new(
        pinset: ClusterPinset,
        rest_endpoint: Option<&str>,
        auth: Option<ClusterAuth>,
        min_replicas: usize,
        limits: RestLimits,
    ) -> Result<Self, VerificationFailure> {
        Self::new_with_network_policy(
            pinset,
            rest_endpoint,
            auth,
            min_replicas,
            limits,
            ClusterNetworkPolicy::PublicOnly,
        )
    }

    /// Only AllowPrivate explicitly authorizes internal endpoints/cleartext HTTP.
    /// Authentication and endpoint identity are never inherited from a Proxy.
    pub fn new_with_network_policy(
        pinset: ClusterPinset,
        rest_endpoint: Option<&str>,
        auth: Option<ClusterAuth>,
        min_replicas: usize,
        limits: RestLimits,
        network_policy: ClusterNetworkPolicy,
    ) -> Result<Self, VerificationFailure> {
        // Revalidate public identity fields so a hand-built value cannot bypass validation.
        let pinset = ClusterPinset::new(&pinset.backend, &pinset.scope)?;
        let endpoint = rest_endpoint
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                VerificationFailure::new(
                    RestOperation::Configure,
                    VerificationCode::MissingRestConfiguration,
                )
            })?;
        let mut base = network::endpoint(endpoint, network_policy).map_err(|_| invalid())?;
        if min_replicas == 0 {
            return Err(invalid());
        }
        limits.validate()?;
        let path = base.path().trim_end_matches('/').to_owned();
        base.set_path(&path);
        Ok(Self {
            pinset,
            base,
            authorization: auth.as_ref().map(ClusterAuth::header).transpose()?,
            min_replicas,
            limits,
            network_policy,
        })
    }

    pub fn resource_key(&self, cid: &str) -> Result<ClusterResourceKey, VerificationFailure> {
        let cid = ::cid::Cid::try_from(cid).map_err(|_| {
            VerificationFailure::new(RestOperation::Verify, VerificationCode::InvalidCid)
        })?;
        Ok(ClusterResourceKey {
            pinset: self.pinset.clone(),
            cid: ::cid::Cid::new_v1(cid.codec(), *cid.hash()).to_string(),
        })
    }
}
