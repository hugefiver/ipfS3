use std::{net::SocketAddr, time::Duration};

use crate::import::ImportConfig;
use anyhow::ensure;
use chrono::Duration as ChronoDuration;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(default = "default_server_config")]
    pub server: ServerConfig,

    #[serde(default = "default_kubo_config")]
    pub kubo: KuboConfig,

    #[serde(default = "default_storage_config")]
    pub storage: StorageConfig,

    #[serde(default = "default_auth_config")]
    pub auth: AuthConfig,

    #[serde(default = "default_crypto_config")]
    pub crypto: CryptoConfig,

    #[serde(default = "default_pinning_config")]
    #[allow(dead_code)]
    pub pinning: PinningConfig,

    #[serde(default)]
    pub imports: ImportConfig,

    #[serde(default)]
    pub lifecycle: LifecycleWorkerConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind: SocketAddr,
}

fn default_bind() -> SocketAddr {
    "0.0.0.0:9000".parse().unwrap()
}

#[derive(Debug, Deserialize, Clone)]
pub struct KuboConfig {
    #[serde(default = "default_rpc_url")]
    pub rpc_url: String,
}

fn default_rpc_url() -> String {
    "http://127.0.0.1:5001".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct StorageConfig {
    #[serde(default = "default_database_url")]
    pub database_url: String,
}

fn default_database_url() -> String {
    "sqlite::memory:".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuthConfig {
    #[serde(default = "default_credentials")]
    pub credentials: Vec<Credential>,
}

fn default_credentials() -> Vec<Credential> {
    vec![Credential {
        access_key: "test".to_string(),
        secret_key: "test".to_string(),
    }]
}

#[derive(Debug, Deserialize, Clone)]
pub struct Credential {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CryptoConfig {
    #[serde(default = "default_master_key")]
    pub master_key: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LifecycleWorkerConfig {
    pub poll_interval_ms: u64,
    pub scan_page_size: u64,
    pub scan_lease_secs: u64,
    pub action_lease_secs: u64,
    pub worker_concurrency: usize,
    pub max_attempts: u64,
    pub base_backoff_secs: u64,
    pub max_backoff_secs: u64,
}

impl Default for LifecycleWorkerConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: 1_000,
            scan_page_size: 500,
            scan_lease_secs: 30,
            action_lease_secs: 30,
            worker_concurrency: 4,
            max_attempts: 8,
            base_backoff_secs: 1,
            max_backoff_secs: 60,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ValidatedLifecycleConfig {
    pub poll_interval: Duration,
    pub scan_page_size: u64,
    pub scan_lease: ChronoDuration,
    pub action_lease: ChronoDuration,
    pub worker_concurrency: usize,
    pub max_attempts: i64,
    pub base_backoff_secs: u64,
    pub max_backoff_secs: u64,
}

impl LifecycleWorkerConfig {
    pub fn validate(&self) -> anyhow::Result<ValidatedLifecycleConfig> {
        use crate::store::{
            lifecycle_action::{
                MAX_LIFECYCLE_ACTION_ATTEMPTS, MAX_LIFECYCLE_ACTION_CLAIM_LIMIT,
                MAX_LIFECYCLE_ACTION_LEASE_SECONDS,
            },
            lifecycle_config::MAX_LIFECYCLE_SCAN_LEASE_SECONDS,
            lifecycle_scan::MAX_LIFECYCLE_SCAN_PAGE_SIZE,
        };

        ensure!(
            self.poll_interval_ms > 0,
            "lifecycle.poll_interval_ms must be non-zero"
        );
        ensure!(
            self.scan_page_size > 0 && self.scan_page_size <= MAX_LIFECYCLE_SCAN_PAGE_SIZE,
            "lifecycle.scan_page_size must be in 1..={MAX_LIFECYCLE_SCAN_PAGE_SIZE}"
        );
        ensure!(
            self.worker_concurrency > 0
                && u64::try_from(self.worker_concurrency)
                    .is_ok_and(|value| value <= MAX_LIFECYCLE_ACTION_CLAIM_LIMIT),
            "lifecycle.worker_concurrency must be in 1..={MAX_LIFECYCLE_ACTION_CLAIM_LIMIT}"
        );
        ensure!(
            self.max_attempts > 0,
            "lifecycle.max_attempts must be non-zero"
        );
        ensure!(
            self.base_backoff_secs > 0,
            "lifecycle.base_backoff_secs must be non-zero"
        );
        ensure!(
            self.base_backoff_secs <= self.max_backoff_secs,
            "lifecycle.base_backoff_secs must not exceed lifecycle.max_backoff_secs"
        );

        let scan_lease_secs = i64::try_from(self.scan_lease_secs)
            .map_err(|_| anyhow::anyhow!("lifecycle.scan_lease_secs is out of range"))?;
        let action_lease_secs = i64::try_from(self.action_lease_secs)
            .map_err(|_| anyhow::anyhow!("lifecycle.action_lease_secs is out of range"))?;
        let max_attempts = i64::try_from(self.max_attempts)
            .map_err(|_| anyhow::anyhow!("lifecycle.max_attempts is out of range"))?;

        ensure!(
            (1..=MAX_LIFECYCLE_SCAN_LEASE_SECONDS).contains(&scan_lease_secs),
            "lifecycle.scan_lease_secs must be in 1..={MAX_LIFECYCLE_SCAN_LEASE_SECONDS}"
        );
        ensure!(
            (1..=MAX_LIFECYCLE_ACTION_LEASE_SECONDS).contains(&action_lease_secs),
            "lifecycle.action_lease_secs must be in 1..={MAX_LIFECYCLE_ACTION_LEASE_SECONDS}"
        );
        ensure!(
            max_attempts <= MAX_LIFECYCLE_ACTION_ATTEMPTS,
            "lifecycle.max_attempts must be in 1..={MAX_LIFECYCLE_ACTION_ATTEMPTS}"
        );

        let scan_lease = ChronoDuration::try_seconds(scan_lease_secs)
            .ok_or_else(|| anyhow::anyhow!("lifecycle.scan_lease_secs is out of range"))?;
        let action_lease = ChronoDuration::try_seconds(action_lease_secs)
            .ok_or_else(|| anyhow::anyhow!("lifecycle.action_lease_secs is out of range"))?;

        Ok(ValidatedLifecycleConfig {
            poll_interval: Duration::from_millis(self.poll_interval_ms),
            scan_page_size: self.scan_page_size,
            scan_lease,
            action_lease,
            worker_concurrency: self.worker_concurrency,
            max_attempts,
            base_backoff_secs: self.base_backoff_secs,
            max_backoff_secs: self.max_backoff_secs,
        })
    }
}

fn default_master_key() -> String {
    "0000000000000000000000000000000000000000000000000000000000000000".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct PinningConfig {
    #[serde(default = "default_worker_interval")]
    pub worker_interval: String,
    #[serde(default = "default_worker_concurrency")]
    pub worker_concurrency: usize,
    #[serde(default)]
    pub providers: Vec<ProviderConfig>,
    #[serde(default)]
    pub policies: Vec<PolicyConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProviderConfig {
    pub name: String,
    pub kind: String,
    pub token_env: Option<String>,
    pub endpoint: Option<String>,
    pub api: Option<String>,
    pub strategy: Option<String>,
    pub upload_endpoint: Option<String>,
    #[serde(default = "enabled")]
    pub enabled: bool,
    pub priority: u32,
    pub max_bytes: u64,
    pub max_pins: u64,
    pub requests_per_second: Option<u32>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PolicyConfig {
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    pub trigger: String,
    pub provider_mode: String,
    pub providers: Vec<String>,
    pub default_duration: String,
    pub max_duration: String,
    #[serde(default)]
    pub allow_decompressed: bool,
}

fn default_worker_interval() -> String {
    "5s".to_owned()
}

fn default_worker_concurrency() -> usize {
    4
}

fn enabled() -> bool {
    true
}

impl Default for PinningConfig {
    fn default() -> Self {
        Self {
            worker_interval: default_worker_interval(),
            worker_concurrency: default_worker_concurrency(),
            providers: Vec::new(),
            policies: Vec::new(),
        }
    }
}

// ---- Default constructors for the Config-level #[serde(default)] ----

fn default_server_config() -> ServerConfig {
    ServerConfig {
        bind: default_bind(),
    }
}

fn default_kubo_config() -> KuboConfig {
    KuboConfig {
        rpc_url: default_rpc_url(),
    }
}

fn default_storage_config() -> StorageConfig {
    StorageConfig {
        database_url: default_database_url(),
    }
}

fn default_auth_config() -> AuthConfig {
    AuthConfig {
        credentials: default_credentials(),
    }
}

fn default_crypto_config() -> CryptoConfig {
    CryptoConfig {
        master_key: default_master_key(),
    }
}

fn default_pinning_config() -> PinningConfig {
    PinningConfig::default()
}

fn default_lifecycle_config() -> LifecycleWorkerConfig {
    LifecycleWorkerConfig::default()
}

// ----------------------------------------------------------------

impl Config {
    fn build_default() -> Self {
        Self {
            server: default_server_config(),
            kubo: default_kubo_config(),
            storage: default_storage_config(),
            auth: default_auth_config(),
            crypto: default_crypto_config(),
            pinning: default_pinning_config(),
            imports: ImportConfig::default(),
            lifecycle: default_lifecycle_config(),
        }
    }

    #[cfg(test)]
    pub(crate) fn default_for_test() -> Self {
        Self::build_default()
    }

    /// Load configuration, with the following precedence (highest last):
    ///
    /// 1. Default values (embedded in code).
    /// 2. TOML file at the path given by `IPFS_S3_CONFIG` env var (defaults to
    ///    `config.toml`).
    /// 3. Individual environment variables:
    ///    - `IPFS_S3_BIND`
    ///    - `IPFS_S3_KUBO_RPC_URL`
    ///    - `IPFS_S3_DATABASE_URL`
    ///    - `IPFS_S3_ACCESS_KEY_ID` + `IPFS_S3_SECRET_ACCESS_KEY` (together
    ///      replace the credentials list).
    ///    - `IPFS_S3_MASTER_KEY` (non-empty values only).
    pub fn load() -> anyhow::Result<Self> {
        let config_path =
            std::env::var("IPFS_S3_CONFIG").unwrap_or_else(|_| "config.toml".to_string());

        let mut config = if std::path::Path::new(&config_path).exists() {
            let content = std::fs::read_to_string(&config_path)?;
            toml::from_str(&content)?
        } else {
            Self::build_default()
        };

        config.apply_env_overrides(|name| std::env::var(name).ok())?;

        Ok(config)
    }

    fn apply_env_overrides<F>(&mut self, get_env: F) -> anyhow::Result<()>
    where
        F: Fn(&str) -> Option<String>,
    {
        if let Some(bind) = get_env("IPFS_S3_BIND") {
            self.server.bind = bind.parse()?;
        }
        if let Some(rpc_url) = get_env("IPFS_S3_KUBO_RPC_URL") {
            self.kubo.rpc_url = rpc_url;
        }
        if let Some(database_url) = get_env("IPFS_S3_DATABASE_URL") {
            self.storage.database_url = database_url;
        }
        if let (Some(access_key), Some(secret_key)) = (
            get_env("IPFS_S3_ACCESS_KEY_ID"),
            get_env("IPFS_S3_SECRET_ACCESS_KEY"),
        ) && !access_key.is_empty()
            && !secret_key.is_empty()
        {
            self.auth.credentials = vec![Credential {
                access_key,
                secret_key,
            }];
        }
        if let Some(master_key) = get_env("IPFS_S3_MASTER_KEY").filter(|value| !value.is_empty()) {
            self.crypto.master_key = master_key;
        }
        if let Some(poll_interval_ms) = get_env("IPFS_S3_LIFECYCLE_POLL_INTERVAL_MS") {
            self.lifecycle.poll_interval_ms = poll_interval_ms.parse()?;
        }
        if let Some(scan_page_size) = get_env("IPFS_S3_LIFECYCLE_SCAN_PAGE_SIZE") {
            self.lifecycle.scan_page_size = scan_page_size.parse()?;
        }
        if let Some(scan_lease_secs) = get_env("IPFS_S3_LIFECYCLE_SCAN_LEASE_SECS") {
            self.lifecycle.scan_lease_secs = scan_lease_secs.parse()?;
        }
        if let Some(action_lease_secs) = get_env("IPFS_S3_LIFECYCLE_ACTION_LEASE_SECS") {
            self.lifecycle.action_lease_secs = action_lease_secs.parse()?;
        }
        if let Some(worker_concurrency) = get_env("IPFS_S3_LIFECYCLE_WORKER_CONCURRENCY") {
            self.lifecycle.worker_concurrency = worker_concurrency.parse()?;
        }
        if let Some(max_attempts) = get_env("IPFS_S3_LIFECYCLE_MAX_ATTEMPTS") {
            self.lifecycle.max_attempts = max_attempts.parse()?;
        }
        if let Some(base_backoff_secs) = get_env("IPFS_S3_LIFECYCLE_BASE_BACKOFF_SECS") {
            self.lifecycle.base_backoff_secs = base_backoff_secs.parse()?;
        }
        if let Some(max_backoff_secs) = get_env("IPFS_S3_LIFECYCLE_MAX_BACKOFF_SECS") {
            self.lifecycle.max_backoff_secs = max_backoff_secs.parse()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILE_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const ENV_KEY: &str = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";

    fn file_config() -> Config {
        let mut config = Config::build_default();
        config.crypto.master_key = FILE_KEY.to_owned();
        config
    }

    #[test]
    fn missing_master_key_env_preserves_file_value() {
        let mut config = file_config();
        config.apply_env_overrides(|_| None).unwrap();
        assert_eq!(config.crypto.master_key, FILE_KEY);
    }

    #[test]
    fn empty_master_key_env_preserves_file_value() {
        let mut config = file_config();
        config
            .apply_env_overrides(|name| (name == "IPFS_S3_MASTER_KEY").then(String::new))
            .unwrap();
        assert_eq!(config.crypto.master_key, FILE_KEY);
    }

    #[test]
    fn non_empty_master_key_env_replaces_file_value() {
        let mut config = file_config();
        config
            .apply_env_overrides(|name| (name == "IPFS_S3_MASTER_KEY").then(|| ENV_KEY.to_owned()))
            .unwrap();
        assert_eq!(config.crypto.master_key, ENV_KEY);
    }

    #[test]
    fn default_pinning_configuration_is_empty() {
        let config = Config::build_default();

        assert_eq!(config.pinning.worker_interval, "5s");
        assert_eq!(config.pinning.worker_concurrency, 4);
        assert!(config.pinning.providers.is_empty());
        assert!(config.pinning.policies.is_empty());
    }

    #[test]
    fn lifecycle_worker_defaults_are_exact() {
        let lifecycle = Config::build_default().lifecycle;

        assert_eq!(lifecycle.poll_interval_ms, 1_000);
        assert_eq!(lifecycle.scan_page_size, 500);
        assert_eq!(lifecycle.scan_lease_secs, 30);
        assert_eq!(lifecycle.action_lease_secs, 30);
        assert_eq!(lifecycle.worker_concurrency, 4);
        assert_eq!(lifecycle.max_attempts, 8);
        assert_eq!(lifecycle.base_backoff_secs, 1);
        assert_eq!(lifecycle.max_backoff_secs, 60);
    }

    #[test]
    fn lifecycle_worker_environment_overrides_all_settings() {
        let mut config = Config::build_default();
        config
            .apply_env_overrides(|name| {
                let value = match name {
                    "IPFS_S3_LIFECYCLE_POLL_INTERVAL_MS" => "2",
                    "IPFS_S3_LIFECYCLE_SCAN_PAGE_SIZE" => "3",
                    "IPFS_S3_LIFECYCLE_SCAN_LEASE_SECS" => "4",
                    "IPFS_S3_LIFECYCLE_ACTION_LEASE_SECS" => "5",
                    "IPFS_S3_LIFECYCLE_WORKER_CONCURRENCY" => "6",
                    "IPFS_S3_LIFECYCLE_MAX_ATTEMPTS" => "7",
                    "IPFS_S3_LIFECYCLE_BASE_BACKOFF_SECS" => "8",
                    "IPFS_S3_LIFECYCLE_MAX_BACKOFF_SECS" => "9",
                    _ => return None,
                };
                Some(value.to_owned())
            })
            .unwrap();

        assert_eq!(config.lifecycle.poll_interval_ms, 2);
        assert_eq!(config.lifecycle.scan_page_size, 3);
        assert_eq!(config.lifecycle.scan_lease_secs, 4);
        assert_eq!(config.lifecycle.action_lease_secs, 5);
        assert_eq!(config.lifecycle.worker_concurrency, 6);
        assert_eq!(config.lifecycle.max_attempts, 7);
        assert_eq!(config.lifecycle.base_backoff_secs, 8);
        assert_eq!(config.lifecycle.max_backoff_secs, 9);
    }

    #[test]
    fn lifecycle_worker_validation_rejects_zero_overflow_and_invalid_backoff() {
        let mut config = LifecycleWorkerConfig::default();
        macro_rules! assert_zero_is_invalid {
            ($field:ident) => {
                config.$field = 0;
                assert!(config.validate().is_err());
                config.$field = 1;
            };
        }
        assert_zero_is_invalid!(poll_interval_ms);
        assert_zero_is_invalid!(scan_page_size);
        assert_zero_is_invalid!(scan_lease_secs);
        assert_zero_is_invalid!(action_lease_secs);
        assert_zero_is_invalid!(max_attempts);

        config.worker_concurrency = 0;
        assert!(config.validate().is_err());
        config.worker_concurrency = 1;

        config.scan_lease_secs = i64::MAX as u64 + 1;
        assert!(config.validate().is_err());
        config.scan_lease_secs = 1;

        config.base_backoff_secs = 0;
        assert!(config.validate().is_err());
        config.base_backoff_secs = 2;
        config.max_backoff_secs = 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn import_defaults_preserve_existing_config_files() {
        let config: Config = toml::from_str(
            r#"
                [server]
                bind = "127.0.0.1:9000"
            "#,
        )
        .unwrap();

        assert!(config.imports.enabled);
        assert!(config.imports.allowed_https_origins.is_empty());
        assert_eq!(config.imports.worker_concurrency, 4);
        assert_eq!(config.imports.poll_interval_ms, 500);
        assert_eq!(config.imports.lease_duration_secs, 60);
        assert_eq!(config.imports.progress_flush_interval_ms, 1_000);
        assert_eq!(config.imports.connect_timeout_secs, 10);
        assert_eq!(config.imports.idle_timeout_secs, 120);
        assert_eq!(config.imports.job_timeout_secs, 86_400);
        assert_eq!(config.imports.max_download_bytes, 5_368_709_120);
        assert_eq!(config.imports.max_attempts, 5);
        assert_eq!(config.imports.terminal_retention_secs, 604_800);
        assert_eq!(config.imports.max_provider_records, 20);
    }

    #[test]
    fn import_origins_require_normalized_https_origins() {
        for origin in [
            "http://example.com",
            "https://user@example.com",
            "https://@example.com",
            "https://example.com/path",
            "https://example.com?query=1",
            "https://example.com#fragment",
            "https://127.0.0.1",
            "https://[::1]",
        ] {
            let config = ImportConfig {
                allowed_https_origins: vec![origin.to_owned()],
                ..ImportConfig::default()
            };
            assert!(config.validate().is_err(), "accepted {origin}");
        }

        let config = ImportConfig {
            allowed_https_origins: vec![
                "https://downloads.example.com".to_owned(),
                "https://downloads.example.com:443".to_owned(),
            ],
            ..ImportConfig::default()
        };
        assert_eq!(config.validate().unwrap().allowed_origins.len(), 1);
    }

    #[test]
    fn empty_import_origin_list_keeps_cid_import_enabled() {
        let validated = ImportConfig::default().validate().unwrap();

        assert!(validated.raw.enabled);
        assert!(validated.allowed_origins.is_empty());
    }

    #[test]
    fn import_numeric_bounds_are_fail_fast() {
        for count in [0, 21] {
            let config = ImportConfig {
                max_provider_records: count,
                ..ImportConfig::default()
            };
            assert!(config.validate().is_err());
        }

        let config = ImportConfig {
            idle_timeout_secs: 0,
            ..ImportConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[tokio::test]
    async fn non_empty_invalid_master_key_env_fails_state_initialization() {
        let mut config = file_config();
        config
            .apply_env_overrides(|name| {
                (name == "IPFS_S3_MASTER_KEY").then(|| "not-hex".to_owned())
            })
            .unwrap();

        let result = crate::state::AppState::new(&config).await;
        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("invalid master key hex")
        );
    }
}
