use std::collections::HashMap;
use std::sync::Arc;

use s3s::auth::SecretKey;

use crate::config::Config;
use crate::crypto::key::MasterKey;
use crate::kubo::KuboClient;
use crate::store::Store;

pub struct AppState {
    pub kubo: KuboClient,
    pub store: Store,
    pub credentials: HashMap<String, SecretKey>,
    pub master_key: MasterKey,
}

impl AppState {
    /// Build a fully-initialized application state from the given
    /// configuration.
    pub async fn new(cfg: &Config) -> anyhow::Result<Arc<Self>> {
        let kubo = KuboClient::new(cfg.kubo.rpc_url.clone());

        let db = sea_orm::Database::connect(&cfg.storage.database_url).await?;
        crate::store::run_migrations(&db).await?;
        let store = Store::new(db);

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
        }))
    }
}
