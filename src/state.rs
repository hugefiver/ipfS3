use std::collections::HashMap;
use std::sync::Arc;

use s3s::auth::SecretKey;

use crate::config::Config;
use crate::crypto::key::MasterKey;
use crate::kubo::KuboClient;
use crate::pinning::{config::ValidatedPinningConfig, coordinator::PinningCoordinator};
use crate::store::Store;

pub struct AppState {
    pub kubo: KuboClient,
    pub store: Store,
    pub credentials: HashMap<String, SecretKey>,
    pub master_key: MasterKey,
    pub pinning: Arc<PinningCoordinator>,
}

impl AppState {
    /// Build a fully-initialized application state from the given
    /// configuration.
    pub async fn new(cfg: &Config) -> anyhow::Result<Arc<Self>> {
        Self::new_with_env(cfg, |name| std::env::var(name).ok()).await
    }

    pub(crate) async fn new_with_env<F>(cfg: &Config, get_env: F) -> anyhow::Result<Arc<Self>>
    where
        F: Fn(&str) -> Option<String>,
    {
        let kubo = KuboClient::new(cfg.kubo.rpc_url.clone());
        let validated_pinning = ValidatedPinningConfig::from_raw(&cfg.pinning, get_env)?;

        let db = sea_orm::Database::connect(&cfg.storage.database_url).await?;
        crate::store::run_migrations(&db).await?;
        let store = Store::new(db);
        let pinning = PinningCoordinator::build_with_kubo(validated_pinning, Some(kubo.clone()))?;

        let credentials: HashMap<String, SecretKey> = cfg
            .auth
            .credentials
            .iter()
            .map(|c| (c.access_key.clone(), SecretKey::from(c.secret_key.as_str())))
            .collect();

        let master_key =
            MasterKey::from_hex(&cfg.crypto.master_key).map_err(|e| anyhow::anyhow!("{e}"))?;

        // Fail-fast on all-zeros master key in release builds. In test/debug
        // builds, warn but allow (so dev environments can boot without config).
        if cfg.crypto.master_key == "0".repeat(64) {
            #[cfg(not(debug_assertions))]
            {
                anyhow::bail!(
                    "master_key is all-zeros — SSE-S3 encryption would provide NO security. \
                     Set IPFS_S3_MASTER_KEY to a strong 32-byte hex key."
                );
            }
            #[cfg(debug_assertions)]
            {
                tracing::warn!(
                    "master_key is all-zeros — SSE-S3 encryption will provide NO real security. \
                     Set IPFS_S3_MASTER_KEY to a strong 32-byte hex key for production."
                );
            }
        }

        Ok(Arc::new(Self {
            kubo,
            store,
            credentials,
            master_key,
            pinning,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::AppState;
    use crate::config::{Config, ProviderConfig};

    #[tokio::test]
    async fn pinning_initialization_rejects_a_missing_provider_token_before_database_connect() {
        let mut config = Config::default_for_test();
        config.storage.database_url = "not-a-database-url".to_owned();
        config.pinning.providers = vec![ProviderConfig {
            name: "pinata".to_owned(),
            kind: "pinata".to_owned(),
            token_env: Some("TASK15_MISSING_PINATA_TOKEN".to_owned()),
            endpoint: None,
            api: None,
            strategy: None,
            upload_endpoint: None,
            enabled: true,
            priority: 1,
            max_bytes: 1_000,
            max_pins: 10,
            requests_per_second: None,
        }];

        let error = match AppState::new_with_env(&config, |_| None).await {
            Ok(_) => panic!("an unresolved provider token must fail before DB setup"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("provider `pinata` token environment variable `TASK15_MISSING_PINATA_TOKEN` is not set"),
            "unexpected startup error: {error:#}"
        );
    }

    #[tokio::test]
    async fn pinning_initialization_builds_an_empty_coordinator_and_cloneable_store() {
        let config = Config::default_for_test();

        let state = AppState::new_with_env(&config, |_| None)
            .await
            .expect("empty pinning configuration must initialize");

        assert!(state.pinning.provider_limits().is_empty());
        let _worker_store = state.store.clone();
    }
}
