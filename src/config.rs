use std::net::SocketAddr;

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

fn default_master_key() -> String {
    "0000000000000000000000000000000000000000000000000000000000000000".to_string()
}

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
pub struct PinningConfig {
    #[serde(default = "default_pinning_provider")]
    pub provider: String,
}

fn default_pinning_provider() -> String {
    "noop".to_string()
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
    PinningConfig {
        provider: default_pinning_provider(),
    }
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
        }
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
