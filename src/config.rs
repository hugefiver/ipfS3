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
    ///    - `IPFS_S3_MASTER_KEY`
    pub fn load() -> anyhow::Result<Self> {
        let config_path =
            std::env::var("IPFS_S3_CONFIG").unwrap_or_else(|_| "config.toml".to_string());

        let mut config = if std::path::Path::new(&config_path).exists() {
            let content = std::fs::read_to_string(&config_path)?;
            toml::from_str(&content)?
        } else {
            Self::build_default()
        };

        // Environment-variable overrides (apply regardless of file existence).
        if let Ok(bind) = std::env::var("IPFS_S3_BIND") {
            config.server.bind = bind.parse()?;
        }
        if let Ok(rpc_url) = std::env::var("IPFS_S3_KUBO_RPC_URL") {
            config.kubo.rpc_url = rpc_url;
        }
        if let Ok(database_url) = std::env::var("IPFS_S3_DATABASE_URL") {
            config.storage.database_url = database_url;
        }
        if let (Ok(access_key), Ok(secret_key)) = (
            std::env::var("IPFS_S3_ACCESS_KEY_ID"),
            std::env::var("IPFS_S3_SECRET_ACCESS_KEY"),
        ) {
            config.auth.credentials = vec![Credential {
                access_key,
                secret_key,
            }];
        }
        if let Ok(master_key) = std::env::var("IPFS_S3_MASTER_KEY") {
            config.crypto.master_key = master_key;
        }

        Ok(config)
    }
}
