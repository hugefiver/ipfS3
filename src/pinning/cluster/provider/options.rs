use super::super::{ClusterAuth, ClusterPinset, RestLimits};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterProtocol {
    Proxy,
    Rest,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterStrategy {
    Cid,
    Upload,
    Car,
}

/// Explicit UnixFS import parameters for byte upload. Different parameters may
/// produce another DAG: mismatch is retained, never followed by a CAR retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterUnixfsOptions {
    pub cid_version: u8,
    pub raw_leaves: bool,
    pub chunk_size: u32,
    pub trickle: bool,
}

/// Stored byte source only; credentials are never inherited from either target.
#[derive(Clone, PartialEq, Eq)]
pub struct ClusterSource {
    pub endpoint: String,
    pub auth: Option<ClusterAuth>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterTransferLimits {
    pub connect_timeout: Duration,
    pub control_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_response_bytes: usize,
    pub max_block_bytes: usize,
    pub max_car_bytes: u64,
    pub max_dag_metadata_bytes: usize,
}
impl Default for ClusterTransferLimits {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(3),
            control_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(30),
            max_response_bytes: 256 * 1024,
            max_block_bytes: 4 * 1024 * 1024,
            max_car_bytes: 16 * 1024 * 1024 * 1024,
            max_dag_metadata_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Administrator-only, independent options; this is NOT another shared registry.
#[derive(Clone, PartialEq, Eq)]
pub struct ClusterProviderOptions {
    pub protocol: ClusterProtocol,
    pub strategy: ClusterStrategy,
    pub proxy_endpoint: Option<String>,
    pub rest_endpoint: String,
    pub proxy_auth: Option<ClusterAuth>,
    pub rest_auth: Option<ClusterAuth>,
    pub source: Option<ClusterSource>,
    pub pinset: ClusterPinset,
    /// Confirmation threshold only. Never sent as replication-min/max.
    pub min_replicas: usize,
    pub allow_private_network: bool,
    pub transfer_limits: ClusterTransferLimits,
    pub verifier_limits: RestLimits,
    /// Official add response mode. false uses a bounded JSON array.
    pub stream_channels: bool,
    /// None uses cid-version from the requested spelling, raw leaves for v1,
    /// balanced layout and size-262144. CAR ignores byte-import parameters.
    pub unixfs: Option<ClusterUnixfsOptions>,
}
impl Default for ClusterProviderOptions {
    fn default() -> Self {
        Self {
            protocol: ClusterProtocol::Rest,
            strategy: ClusterStrategy::Cid,
            proxy_endpoint: None,
            rest_endpoint: String::new(),
            proxy_auth: None,
            rest_auth: None,
            source: None,
            pinset: ClusterPinset {
                backend: String::new(),
                scope: String::new(),
            },
            min_replicas: 1,
            allow_private_network: false,
            transfer_limits: ClusterTransferLimits::default(),
            verifier_limits: RestLimits::default(),
            stream_channels: true,
            unixfs: None,
        }
    }
}

impl ClusterProviderOptions {
    pub(crate) fn validate(&self) -> Result<(), super::ClusterWriteFailure> {
        use super::super::{ClusterNetworkPolicy, network};
        use super::{ClusterFailureCode as Code, ClusterWriteFailure as Failure};
        let policy = ClusterNetworkPolicy::from_allow_private_network(self.allow_private_network);
        let endpoint = |value: &str| {
            network::endpoint(value, policy).map_err(|_| Failure::new(Code::InvalidConfiguration))
        };
        endpoint(&self.rest_endpoint)?;
        match self.protocol {
            ClusterProtocol::Proxy => {
                endpoint(
                    self.proxy_endpoint
                        .as_deref()
                        .ok_or_else(|| Failure::new(Code::InvalidConfiguration))?,
                )?;
            }
            ClusterProtocol::Rest if self.proxy_endpoint.is_some() || self.proxy_auth.is_some() => {
                return Err(Failure::new(Code::InvalidConfiguration));
            }
            _ => {}
        }
        if self.strategy != ClusterStrategy::Cid && self.source.is_none() {
            return Err(Failure::new(Code::InvalidConfiguration));
        }
        if let Some(source) = &self.source {
            let source = endpoint(&source.endpoint)?;
            let normalize = |mut url: reqwest::Url| {
                let path = url
                    .path()
                    .trim_end_matches('/')
                    .trim_end_matches("/api/v0")
                    .to_owned();
                url.set_path(&path);
                url
            };
            let source = normalize(source);
            for target in [
                Some(self.rest_endpoint.as_str()),
                self.proxy_endpoint.as_deref(),
            ]
            .into_iter()
            .flatten()
            {
                if normalize(endpoint(target)?) == source {
                    return Err(Failure::new(Code::InvalidConfiguration));
                }
            }
        }
        let limits = self.transfer_limits;
        if self
            .unixfs
            .is_some_and(|u| u.cid_version > 1 || u.chunk_size == 0 || u.chunk_size > 1024 * 1024)
        {
            return Err(Failure::new(Code::InvalidConfiguration));
        }
        if [
            limits.connect_timeout,
            limits.control_timeout,
            limits.idle_timeout,
        ]
        .iter()
        .any(|d| d.is_zero() || *d > Duration::from_secs(86_400))
            || limits.connect_timeout > limits.control_timeout
            || limits.max_response_bytes == 0
            || limits.max_response_bytes > 4 * 1024 * 1024
            || limits.max_block_bytes == 0
            || limits.max_block_bytes > 16 * 1024 * 1024
            || limits.max_car_bytes == 0
            || limits.max_dag_metadata_bytes == 0
            || limits.max_dag_metadata_bytes > 256 * 1024 * 1024
            || self.min_replicas == 0
            || self.min_replicas > 1024
        {
            return Err(Failure::new(Code::InvalidConfiguration));
        }
        Ok(())
    }
}

impl std::fmt::Debug for ClusterProviderOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterProviderOptions")
            .field("protocol", &self.protocol)
            .field("strategy", &self.strategy)
            .field("pinset", &self.pinset)
            .field("min_replicas", &self.min_replicas)
            .field("allow_private_network", &self.allow_private_network)
            .finish_non_exhaustive()
    }
}

impl ClusterProtocol {
    pub fn api_profile(self) -> &'static str {
        match self {
            Self::Proxy => "cluster-proxy",
            Self::Rest => "cluster-rest",
        }
    }
}
impl ClusterStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cid => "cid",
            Self::Upload => "upload",
            Self::Car => "car",
        }
    }
}

impl ClusterProviderOptions {
    pub fn confirmation(&self) -> crate::pinning::identity::ConfirmationRequirement {
        crate::pinning::identity::ConfirmationRequirement::ClusterReplicas {
            min_replicas: self.min_replicas as u32,
            max_age_secs: self.verifier_limits.max_observation_age.as_secs() as u32,
        }
    }
}
