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
    pub cold_kubo: Option<KuboClient>,
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
        cfg.validate_kubo()?;
        let kubo = KuboClient::new(cfg.kubo.rpc_url.clone());
        let cold_kubo = cfg
            .cold_kubo
            .as_ref()
            .map(|config| KuboClient::new(config.rpc_url.clone()));
        let validated_pinning = ValidatedPinningConfig::from_config(cfg, get_env)?;

        let db = crate::store::connect_database(&cfg.storage.database_url).await?;
        crate::store::run_migrations(&db).await?;
        let store = Store::new(db);
        let pinning = PinningCoordinator::build_with_kubo_and_mode(
            validated_pinning,
            Some(kubo.clone()),
            cfg.pinning_control.unavailable,
        )?;
        pinning.register_identities(&store).await?;

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
            cold_kubo,
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
    use crate::{
        config::{Config, OptionalPinControlMode, PolicyConfig, ProviderConfig},
        pinning::{
            decision::{DecisionEffect, DecisionOrigin, WarningCode},
            policy::PublicationContext,
            tags::ObjectTag,
        },
    };
    use std::cell::Cell;

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
        assert!(state.cold_kubo.is_none());
        let _worker_store = state.store.clone();
    }

    #[tokio::test]
    async fn new_with_env_keeps_warn_mode_and_the_single_resolved_secret() {
        let mut config = Config::default_for_test();
        config.pinning_control.unavailable = OptionalPinControlMode::Warn;
        config.pinning.providers = vec![ProviderConfig {
            name: "pinata".to_owned(),
            kind: "pinata".to_owned(),
            token_env: Some("PINATA_TOKEN".to_owned()),
            endpoint: None,
            api: None,
            strategy: None,
            upload_endpoint: None,
            enabled: false,
            priority: 1,
            max_bytes: 1_000,
            max_pins: 10,
            requests_per_second: None,
        }];
        config.pinning.policies = vec![PolicyConfig {
            bucket: "bucket".to_owned(),
            prefix: String::new(),
            trigger: "request".to_owned(),
            provider_mode: "one".to_owned(),
            providers: vec!["pinata".to_owned()],
            default_duration: "1h".to_owned(),
            max_duration: "2h".to_owned(),
            allow_decompressed: false,
        }];
        let resolutions = Cell::new(0);
        let secret = "private-token-value";
        let state = AppState::new_with_env(&config, |name| {
            assert_eq!(name, "PINATA_TOKEN");
            resolutions.set(resolutions.get() + 1);
            Some(secret.to_owned())
        })
        .await
        .unwrap();
        assert_eq!(resolutions.get(), 1);
        assert_eq!(state.pinning.control_mode(), OptionalPinControlMode::Warn);
        assert_eq!(
            state.pinning.effective_config().providers[0]
                .token
                .as_ref()
                .unwrap()
                .expose(),
            secret
        );
        assert!(!format!("{:?}", state.pinning.effective_config()).contains(secret));
        assert!(!state.pinning.provider_limits()["pinata"].enabled);
        let tags = [ObjectTag::new("ipfs-s3:pin", "true")];
        let (policy, decision) = state
            .pinning
            .policy()
            .evaluate_publication_decision(
                PublicationContext {
                    bucket: "bucket",
                    key: "object",
                    tags: &tags,
                    is_decompress_zip: false,
                },
                DecisionOrigin::new("principal", "request"),
            )
            .unwrap();
        assert!(policy.leases.is_empty());
        assert_eq!(decision.effect, DecisionEffect::Skipped);
        assert_eq!(decision.warning, Some(WarningCode::NoAvailableProvider));
    }

    #[tokio::test]
    async fn unavailable_optional_cold_does_not_block_startup() {
        let mut config = Config::default_for_test();
        config.cold_kubo = Some(crate::config::KuboConfig {
            rpc_url: "http://127.0.0.1:1".to_owned(),
        });
        let state = AppState::new_with_env(&config, |_| None).await.unwrap();
        assert!(state.cold_kubo.is_some());
        assert!(state.pinning.provider_limits().is_empty());
    }
}
