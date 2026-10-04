use std::{net::SocketAddr, time::Duration};

use crate::import::ImportConfig;
use anyhow::ensure;
use chrono::Duration as ChronoDuration;
use serde::Deserialize;

use crate::pinning::identity::PinningIdentityConfig;
use crate::pinning::zip_policy::ZipOutputRuleConfig;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(default = "default_server_config")]
    pub server: ServerConfig,

    #[serde(default = "default_kubo_config")]
    pub kubo: KuboConfig,

    #[serde(default)]
    pub cold_kubo: Option<KuboConfig>,

    #[serde(default = "default_storage_config")]
    pub storage: StorageConfig,

    #[serde(default = "default_auth_config")]
    pub auth: AuthConfig,

    #[serde(default = "default_crypto_config")]
    pub crypto: CryptoConfig,

    #[serde(default = "default_pinning_config")]
    #[allow(dead_code)]
    pub pinning: PinningConfig,

    /// Compatibility opt-in for *optional manual* pin controls only. Omission is strict.
    #[serde(default)]
    pub pinning_control: PinningControlConfig,

    #[serde(default)]
    pub pinning_identity: PinningIdentityConfig,

    /// RPC-only options keyed by provider config_name; legacy provider literals
    /// and TOML remain source/schema compatible.
    #[serde(default)]
    pub pinning_rpc: crate::pinning::ipfs_rpc::RpcProviderRegistry,

    #[serde(default)]
    pub imports: ImportConfig,

    #[serde(default)]
    pub lifecycle: LifecycleWorkerConfig,

    #[serde(default)]
    pub decompress_zip: DecompressZipConfig,
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

/// Default for ZIP decompression requests only; ordinary object operations do not use it.
#[derive(Debug, Deserialize, Clone)]
pub struct DecompressZipConfig {
    #[serde(default = "default_zip_root_enabled")]
    pub unixfs_directory_root: bool,
    /// ZIP extraction budgets, declared directly under `[decompress_zip]`.
    /// The request/import entrypoints must pass these limits explicitly; the
    /// extractor's legacy entrypoints intentionally continue using defaults.
    #[serde(flatten)]
    pub limits: crate::zip::extract::ZipExtractionLimits,
    /// Optional ZIP v2-only rules ([[decompress_zip.pin_output_rules]]).
    /// The pure engine must be explicitly compiled/admitted by a v2 caller;
    /// merely configuring a rule does not authorize or enqueue remote work.
    #[serde(default)]
    pub pin_output_rules: Vec<ZipOutputRuleConfig>,
}

fn default_zip_root_enabled() -> bool {
    true
}

impl Default for DecompressZipConfig {
    fn default() -> Self {
        Self {
            unixfs_directory_root: default_zip_root_enabled(),
            limits: crate::zip::extract::ZipExtractionLimits::default(),
            pin_output_rules: Vec::new(),
        }
    }
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionalPinControlMode {
    #[default]
    Strict,
    Warn,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PinningControlConfig {
    #[serde(default)]
    pub unavailable: OptionalPinControlMode,
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
            cold_kubo: None,
            storage: default_storage_config(),
            auth: default_auth_config(),
            crypto: default_crypto_config(),
            pinning: default_pinning_config(),
            pinning_control: PinningControlConfig::default(),
            pinning_identity: PinningIdentityConfig::default(),
            pinning_rpc: crate::pinning::ipfs_rpc::RpcProviderRegistry::default(),
            imports: ImportConfig::default(),
            lifecycle: default_lifecycle_config(),
            decompress_zip: DecompressZipConfig::default(),
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
    ///    - `IPFS_S3_COLD_KUBO_RPC_URL` (optional independent node)
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
        config.validate_kubo()?;

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
        if let Some(rpc_url) = get_env("IPFS_S3_COLD_KUBO_RPC_URL") {
            self.cold_kubo = Some(KuboConfig { rpc_url });
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

    pub(crate) fn validate_kubo(&self) -> anyhow::Result<()> {
        for (config, message) in [
            (Some(&self.kubo), "invalid hot Kubo RPC URL"),
            (self.cold_kubo.as_ref(), "invalid cold Kubo RPC URL"),
        ] {
            if let Some(config) = config {
                let valid = reqwest::Url::parse(&config.rpc_url).is_ok_and(|url| {
                    matches!(url.scheme(), "http" | "https")
                        && url.host_str().is_some()
                        && url.query().is_none()
                        && url.fragment().is_none()
                });
                ensure!(valid, message);
            }
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
    fn cold_kubo_is_optional_and_env_does_not_replace_hot() {
        let mut config: Config = toml::from_str("").unwrap();
        assert!(config.cold_kubo.is_none());
        let hot = config.kubo.rpc_url.clone();
        config
            .apply_env_overrides(|name| {
                (name == "IPFS_S3_COLD_KUBO_RPC_URL").then(|| "http://cold:5001".to_owned())
            })
            .unwrap();
        assert_eq!(config.kubo.rpc_url, hot);
        assert_eq!(config.cold_kubo.unwrap().rpc_url, "http://cold:5001");
    }

    #[test]
    fn cold_kubo_toml_and_validation_are_redacted() {
        let config: Config = toml::from_str("[cold_kubo]\nrpc_url = 'http://cold:5001'").unwrap();
        config.validate_kubo().unwrap();
        assert_eq!(config.cold_kubo.unwrap().rpc_url, "http://cold:5001");
        for url in [
            "",
            "file:///private",
            "http://user:secret@",
            "not-secret-url",
        ] {
            let mut config = Config::build_default();
            config.cold_kubo = Some(KuboConfig {
                rpc_url: url.to_owned(),
            });
            let error = config.validate_kubo().unwrap_err();
            assert_eq!(error.to_string(), "invalid cold Kubo RPC URL");
            assert!(!format!("{error:?}").contains("secret"));
        }
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
    fn optional_pinning_compatibility_requires_explicit_warn() {
        let legacy: Config = toml::from_str("").unwrap();
        assert_eq!(
            legacy.pinning_control.unavailable,
            OptionalPinControlMode::Strict
        );
        let warn: Config = toml::from_str("[pinning_control]\nunavailable = 'warn'").unwrap();
        assert_eq!(
            warn.pinning_control.unavailable,
            OptionalPinControlMode::Warn
        );
        assert!(toml::from_str::<Config>("[pinning_control]\nunavailable = 'ignore'").is_err());
    }

    #[test]
    fn provider_identity_configuration_defaults_to_legacy_compatibility() {
        let config: Config = toml::from_str("").unwrap();

        assert!(config.pinning_identity.primary_storage_domain.is_none());
        assert!(config.pinning_identity.providers.is_empty());
    }

    #[test]
    fn provider_identity_configuration_deserializes_without_secrets() {
        let config: Config = toml::from_str(
            r#"
                [pinning_identity]
                primary_storage_domain = "kubo:primary"

                [[pinning_identity.providers]]
                config_name = "pinata-primary"
                provider_id = "pinata-prod"
                display_name = "Primary Pinata"
                backend = "pinata"
                scope = "account:prod"
                storage_domain = "pinata:prod"
                credential_revision = 2
                endpoint_revision = 3
                secret_ref = "env:PINATA_JWT"
                api_profile = "pinata-v3"
                strategy = "cid"
                retired = true
            "#,
        )
        .unwrap();

        let identity = &config.pinning_identity.providers[0];
        assert_eq!(identity.config_name, "pinata-primary");
        assert_eq!(identity.provider_id, "pinata-prod");
        assert_eq!(identity.display_name, "Primary Pinata");
        assert_eq!(identity.credential_revision, 2);
        assert_eq!(identity.endpoint_revision, 3);
        assert!(identity.retired);
        assert_eq!(
            identity.cleanup,
            crate::pinning::identity::CleanupMode::Retain
        );
    }

    #[test]
    fn example_configuration_has_a_valid_explicit_provider_identity_registry() {
        let config: Config = toml::from_str(include_str!("../config.example.toml")).unwrap();
        let validated =
            crate::pinning::config::ValidatedPinningConfig::from_config(&config, |name| {
                Some(format!("test-token-for-{name}"))
            })
            .unwrap();

        assert_eq!(validated.providers.len(), 2);
        assert!(
            validated
                .providers
                .iter()
                .all(|provider| provider.identity.cleanup
                    == crate::pinning::identity::CleanupMode::Retain)
        );
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
    fn zip_root_configuration_defaults_on_and_accepts_explicit_false() {
        let legacy: Config = toml::from_str("[server]\nbind = '127.0.0.1:9000'").unwrap();
        assert!(legacy.decompress_zip.unixfs_directory_root);
        assert!(Config::build_default().decompress_zip.unixfs_directory_root);
        let empty_section: Config = toml::from_str("[decompress_zip]").unwrap();
        assert!(empty_section.decompress_zip.unixfs_directory_root);

        let off: Config =
            toml::from_str("[decompress_zip]\nunixfs_directory_root = false").unwrap();
        assert!(!off.decompress_zip.unixfs_directory_root);
        let on: Config = toml::from_str("[decompress_zip]\nunixfs_directory_root = true").unwrap();
        assert!(on.decompress_zip.unixfs_directory_root);
        assert!(
            toml::from_str::<Config>("[decompress_zip]\nunixfs_directory_root = 'false'").is_err()
        );

        let example: Config = toml::from_str(include_str!("../config.example.toml")).unwrap();
        assert!(example.decompress_zip.unixfs_directory_root);
    }

    #[test]
    fn zip_limits_default_and_configured_bounds() {
        let defaults: Config = toml::from_str("[decompress_zip]").unwrap();
        assert_eq!(
            defaults.decompress_zip.limits.max_decompressed_bytes(),
            crate::zip::extract::MAX_DECOMPRESSED_ARCHIVE_BYTES
        );
        assert_eq!(
            defaults.decompress_zip.limits.max_single_entry_bytes(),
            crate::zip::extract::MAX_DECOMPRESSED_ARCHIVE_BYTES
        );
        let configured: Config = toml::from_str(
            "[decompress_zip]\nmax_decompressed_bytes = 7\nmax_single_entry_bytes = 2\nmax_entries = 3\nmax_metadata_bytes = 4096\nmax_staged_adds = 1\nprocessing_deadline_secs = 120",
        )
        .unwrap();
        assert_eq!(configured.decompress_zip.limits.max_decompressed_bytes(), 7);
        assert_eq!(configured.decompress_zip.limits.max_single_entry_bytes(), 2);
        assert_eq!(configured.decompress_zip.limits.max_entries(), 3);
        assert_eq!(configured.decompress_zip.limits.max_metadata_bytes(), 4096);
        assert_eq!(configured.decompress_zip.limits.max_staged_adds(), 1);
        assert_eq!(
            configured.decompress_zip.limits.processing_deadline_secs(),
            Some(120)
        );
        for invalid in [
            "max_entries = 0",
            "max_entries = 100001",
            "max_decompressed_bytes = 1099511627777",
            "max_single_entry_bytes = 1099511627777",
            "max_metadata_bytes = 1073741825",
            "max_staged_adds = 100001",
            "processing_deadline_secs = 0",
            "processing_deadline_secs = 604801",
        ] {
            assert!(
                toml::from_str::<Config>(&format!("[decompress_zip]\n{invalid}")).is_err(),
                "accepted {invalid}"
            );
        }
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
