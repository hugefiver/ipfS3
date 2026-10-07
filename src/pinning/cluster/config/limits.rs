use crate::pinning::cluster::{RestLimits, provider::ClusterTransferLimits};
use anyhow::ensure;
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterTransferConfig {
    pub connect_timeout_seconds: Option<u64>,
    pub control_timeout_seconds: Option<u64>,
    pub idle_timeout_seconds: Option<u64>,
    pub max_response_bytes: Option<usize>,
    pub max_block_bytes: Option<usize>,
    pub max_car_bytes: Option<u64>,
    pub max_dag_metadata_bytes: Option<usize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterVerifierConfig {
    pub connect_timeout_seconds: Option<u64>,
    pub total_timeout_seconds: Option<u64>,
    pub idle_timeout_seconds: Option<u64>,
    pub max_body_bytes: Option<usize>,
    pub max_age_secs: Option<u32>,
}

fn duration(raw: Option<u64>, default: Duration, ceiling: u64) -> anyhow::Result<Duration> {
    let secs = raw.unwrap_or(default.as_secs());
    ensure!(
        (1..=ceiling).contains(&secs),
        "Cluster timeout is outside its bounded range"
    );
    Ok(Duration::from_secs(secs))
}

impl ClusterTransferConfig {
    pub(super) fn validate(&self) -> anyhow::Result<ClusterTransferLimits> {
        let d = ClusterTransferLimits::default();
        Ok(ClusterTransferLimits {
            connect_timeout: duration(self.connect_timeout_seconds, d.connect_timeout, 86_400)?,
            control_timeout: duration(self.control_timeout_seconds, d.control_timeout, 86_400)?,
            idle_timeout: duration(self.idle_timeout_seconds, d.idle_timeout, 86_400)?,
            max_response_bytes: self.max_response_bytes.unwrap_or(d.max_response_bytes),
            max_block_bytes: self.max_block_bytes.unwrap_or(d.max_block_bytes),
            max_car_bytes: self.max_car_bytes.unwrap_or(d.max_car_bytes),
            max_dag_metadata_bytes: self
                .max_dag_metadata_bytes
                .unwrap_or(d.max_dag_metadata_bytes),
        })
    }
}
impl ClusterVerifierConfig {
    pub(super) fn validate(&self) -> anyhow::Result<RestLimits> {
        let d = RestLimits::default();
        Ok(RestLimits {
            connect_timeout: duration(self.connect_timeout_seconds, d.connect_timeout, 300)?,
            total_timeout: duration(self.total_timeout_seconds, d.total_timeout, 300)?,
            idle_timeout: duration(self.idle_timeout_seconds, d.idle_timeout, 300)?,
            max_body_bytes: self.max_body_bytes.unwrap_or(d.max_body_bytes),
            max_observation_age: duration(
                self.max_age_secs.map(u64::from),
                d.max_observation_age,
                300,
            )?,
        })
    }
}
