use std::collections::HashSet;

use anyhow::{Context, bail, ensure};
use serde::Deserialize;
use url::{Host, Origin, Url};

pub mod decompress;
pub mod downloader;
pub mod error;
mod execution_error;
pub mod model;
pub mod pipeline;
mod progress;
mod publication;
pub mod response;
mod source;
pub mod v2_source;
mod v2_worker;
pub mod worker;

pub use model::*;

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ImportConfig {
    pub enabled: bool,
    pub allowed_https_origins: Vec<String>,
    pub worker_concurrency: usize,
    pub poll_interval_ms: u64,
    pub lease_duration_secs: u64,
    pub progress_flush_interval_ms: u64,
    pub connect_timeout_secs: u64,
    pub idle_timeout_secs: u64,
    pub job_timeout_secs: u64,
    pub max_download_bytes: u64,
    pub max_attempts: u32,
    pub terminal_retention_secs: u64,
    pub max_provider_records: usize,
}

impl Default for ImportConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            allowed_https_origins: Vec::new(),
            worker_concurrency: 4,
            poll_interval_ms: 500,
            lease_duration_secs: 60,
            progress_flush_interval_ms: 1_000,
            connect_timeout_secs: 10,
            idle_timeout_secs: 120,
            job_timeout_secs: 86_400,
            max_download_bytes: 5_368_709_120,
            max_attempts: 5,
            terminal_retention_secs: 604_800,
            max_provider_records: 20,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ValidatedImportConfig {
    pub raw: ImportConfig,
    pub allowed_origins: HashSet<Origin>,
}

impl ImportConfig {
    pub fn validate(&self) -> anyhow::Result<ValidatedImportConfig> {
        ensure!(
            self.worker_concurrency > 0,
            "imports.worker_concurrency must be non-zero"
        );
        ensure!(
            self.poll_interval_ms > 0,
            "imports.poll_interval_ms must be non-zero"
        );
        ensure!(
            self.lease_duration_secs > 0,
            "imports.lease_duration_secs must be non-zero"
        );
        ensure!(
            self.progress_flush_interval_ms > 0,
            "imports.progress_flush_interval_ms must be non-zero"
        );
        ensure!(
            self.connect_timeout_secs > 0,
            "imports.connect_timeout_secs must be non-zero"
        );
        ensure!(
            self.idle_timeout_secs > 0,
            "imports.idle_timeout_secs must be non-zero"
        );
        ensure!(
            self.job_timeout_secs > 0,
            "imports.job_timeout_secs must be non-zero"
        );
        ensure!(
            self.max_download_bytes > 0,
            "imports.max_download_bytes must be non-zero"
        );
        ensure!(
            self.max_attempts > 0,
            "imports.max_attempts must be non-zero"
        );
        ensure!(
            self.terminal_retention_secs > 0,
            "imports.terminal_retention_secs must be non-zero"
        );
        ensure!(
            (1..=20).contains(&self.max_provider_records),
            "imports.max_provider_records must be in 1..=20"
        );

        let mut allowed_origins = HashSet::new();
        for configured_origin in &self.allowed_https_origins {
            let mut url = Url::parse(configured_origin)
                .with_context(|| "invalid imports.allowed_https_origins entry")?;

            if url.scheme() != "https" {
                bail!("imports.allowed_https_origins entries must use https");
            }
            let raw_authority = configured_origin
                .split_once("://")
                .map(|(_, authority)| authority)
                .unwrap_or_default();
            let authority_end = raw_authority
                .find(['/', '?', '#'])
                .unwrap_or(raw_authority.len());
            if raw_authority[..authority_end].contains('@') {
                bail!("imports.allowed_https_origins entries must not contain userinfo");
            }
            if url.path() != "/" {
                bail!("imports.allowed_https_origins entries must not contain a path");
            }
            if url.query().is_some() {
                bail!("imports.allowed_https_origins entries must not contain a query");
            }
            if url.fragment().is_some() {
                bail!("imports.allowed_https_origins entries must not contain a fragment");
            }
            if !matches!(url.host(), Some(Host::Domain(_))) {
                bail!("imports.allowed_https_origins entries must use a DNS hostname");
            }

            if url.port() == Some(443) {
                url.set_port(None)
                    .map_err(|_| anyhow::anyhow!("failed to normalize HTTPS port"))?;
            }
            allowed_origins.insert(url.origin());
        }

        Ok(ValidatedImportConfig {
            raw: self.clone(),
            allowed_origins,
        })
    }
}
