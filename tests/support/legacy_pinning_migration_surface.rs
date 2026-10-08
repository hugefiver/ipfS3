//! Owned file-SQLite / real gateway fixture. No worker is ever started.

use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use ipfs_s3_gateway::{config::Config, pinning::identity::ProviderIdentity, state::AppState};
use sea_orm::Database;
use serde_json::Value;
use tokio::process::Command;

use crate::support::decompress::{
    AddReply, KuboScript, S3ServerHandle, S3TestEndpoint, legal_single_entry_zip,
    start_kubo_harness, start_s3_server,
};

#[path = "legacy_pinning_migration_surface_db.rs"]
pub mod db;

pub const DATA_CID: &str = "QmSurfaceData";
pub const PART_CID: &str = "QmSurfacePart";
pub const ARCHIVE_CID: &str = "QmSurfaceArchive";
pub const ENTRY_CID: &str = "QmSurfaceEntry";
pub const OLD_CID: &str = "QmSurfaceHistorical";
pub const BODY: &[u8] = b"body";
pub const ENTRY: &[u8] = b"single entry bytes";
pub const BUCKET: &str = "test-bkt";

#[derive(Clone, Copy, Debug)]
pub enum Blocker {
    UsageOnly,
    // Usage is exactly the remote's 7 bytes / 1 pin, not an under-accounted
    // remote-only migration fixture. Independent guard evidence lives in
    // pinning_stage2_migration, not in this combination HTTP case.
    AccountedRemote,
    DomainNullRoute,
}

impl Blocker {
    pub fn capacity(self) -> (i64, i64) {
        match self {
            Self::AccountedRemote => (7, 1),
            Self::UsageOnly | Self::DomainNullRoute => (97, 3),
        }
    }
}

pub struct Fixture {
    directory: tempfile::TempDir,
    config: Config,
    config_file: PathBuf,
    pub database_url: String,
    pub identity: ProviderIdentity,
    pub blocker: Blocker,
    pub kubo: wiremock::MockServer,
    state: Option<Arc<AppState>>,
    server: Option<S3ServerHandle>,
}

impl S3TestEndpoint for Fixture {
    fn endpoint(&self) -> &str {
        &self.server.as_ref().expect("gateway is running").endpoint
    }

    fn bucket(&self) -> &str {
        BUCKET
    }
}

impl Fixture {
    pub async fn new(blocker: Blocker) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            directory
                .path()
                .join("surface.sqlite")
                .display()
                .to_string()
                .replace('\\', "/")
        );
        // Source PUT, MPU part, then before/after PUT + Complete + ZIP archive
        // and ZIP entry. Copy shares the source CID without adding content.
        let kubo = start_kubo_harness(KuboScript {
            add_replies: [
                DATA_CID,
                PART_CID,
                DATA_CID,
                DATA_CID,
                ARCHIVE_CID,
                ENTRY_CID,
                DATA_CID,
                DATA_CID,
                ARCHIVE_CID,
                ENTRY_CID,
            ]
            .into_iter()
            .map(AddReply::Ok)
            .collect(),
            cat_bodies: HashMap::from([
                (DATA_CID.into(), BODY.to_vec()),
                (PART_CID.into(), BODY.to_vec()),
                (ARCHIVE_CID.into(), legal_single_entry_zip()),
                (ENTRY_CID.into(), ENTRY.to_vec()),
            ]),
        })
        .await
        .server;
        let text = format!(
            r#"
[storage]
database_url = {database_url:?}
[kubo]
rpc_url = {kubo_url:?}
[auth]
[[auth.credentials]]
access_key = 'test'
secret_key = 'test'
[pinning_control]
unavailable = 'strict'
[pinning_identity]
primary_storage_domain = 'owned-surface-kubo'
[[pinning_identity.providers]]
config_name = 'surface'
provider_id = 'surface-account'
display_name = 'Surface Noop'
backend = 'noop'
scope = 'owned-surface-account'
storage_domain = 'owned-surface-remote'
credential_revision = 1
endpoint_revision = 1
api_profile = 'noop'
strategy = 'cid'
cleanup = 'retain'
[[pinning.providers]]
name = 'surface'
kind = 'noop'
priority = 1
max_bytes = 1000
max_pins = 10
[[pinning.policies]]
bucket = 'test-bkt'
prefix = ''
trigger = 'request'
provider_mode = 'one'
providers = ['surface']
default_duration = '1h'
max_duration = '30d'
allow_decompressed = true
"#,
            kubo_url = kubo.uri()
        );
        let config: Config = toml::from_str(&text).unwrap();
        let config_file = directory.path().join("gateway.toml");
        std::fs::write(&config_file, text).unwrap();
        let state = AppState::new(&config).await.unwrap();
        let identity = state.pinning.effective_config().providers[0]
            .identity
            .clone();
        ipfs_s3_gateway::store::bucket::create(state.store.db(), BUCKET, None)
            .await
            .unwrap();
        db::seed(state.store.db(), blocker, &identity).await;
        let server = start_s3_server(state.clone(), Arc::default()).await;
        Self {
            directory,
            config,
            config_file,
            database_url,
            identity,
            blocker,
            kubo,
            state: Some(state),
            server: Some(server),
        }
    }

    pub fn db(&self) -> &sea_orm::DatabaseConnection {
        self.state.as_ref().expect("gateway state").store.db()
    }

    pub async fn stop(&mut self) {
        self.server.take().expect("gateway server").shutdown().await;
        let state = self.state.take().expect("gateway state");
        assert_eq!(Arc::strong_count(&state), 1, "HTTP writers must be drained");
        state.store.db().clone().close().await.unwrap();
    }

    pub async fn restart(&mut self) {
        assert!(self.server.is_none() && self.state.is_none());
        let state = AppState::new(&self.config).await.unwrap();
        self.server = Some(start_s3_server(state.clone(), Arc::default()).await);
        self.state = Some(state);
    }

    pub async fn migrate(&self) -> Value {
        assert!(
            self.server.is_none() && self.state.is_none(),
            "never attest a live writer"
        );
        let declaration_file = self.directory.path().join("declaration.json");
        let confirmation_file = self.directory.path().join("confirmation.json");
        let readonly_url = self.database_url.replace("?mode=rwc", "?mode=ro");
        let connection = Database::connect(&readonly_url).await.unwrap();
        let before = db::snapshot(&connection).await;
        connection.close().await.unwrap();
        // Only now, with the HTTP server gone, DB pool closed, and no worker
        // ever created, are all_writers_stopped/all_workers_drained truthful.
        let declaration =
            db::declaration(&before, self.blocker, &self.identity, &confirmation_file);
        std::fs::write(
            &declaration_file,
            serde_json::to_vec_pretty(&declaration).unwrap(),
        )
        .unwrap();
        let requests = self.kubo.received_requests().await.unwrap().len();
        let dry = self.cli(&declaration_file, None).await;
        assert_eq!(dry["mode"], "dry_run");
        let confirmation =
            serde_json::from_slice(&std::fs::read(&confirmation_file).unwrap()).unwrap();
        let connection = Database::connect(&readonly_url).await.unwrap();
        assert_eq!(
            db::snapshot(&connection).await,
            before,
            "dry-run is DB read-only"
        );
        db::assert_confirmation(&connection, &confirmation, false).await;
        connection.close().await.unwrap();
        let receipt = self.cli(&declaration_file, Some(&confirmation_file)).await;
        assert_eq!(receipt["direction"], "apply");
        let connection = Database::connect(&readonly_url).await.unwrap();
        db::assert_confirmation(&connection, &confirmation, true).await;
        db::assert_migrated(&connection, self.blocker, &self.identity).await;
        let after = db::snapshot(&connection).await;
        connection.close().await.unwrap();
        assert_eq!(
            self.cli(&declaration_file, Some(&confirmation_file)).await,
            receipt
        );
        let connection = Database::connect(&readonly_url).await.unwrap();
        assert_eq!(
            db::snapshot(&connection).await,
            after,
            "replay must not move quota or duplicate receipt items"
        );
        connection.close().await.unwrap();
        assert_eq!(
            self.kubo.received_requests().await.unwrap().len(),
            requests,
            "CLI must not perform Kubo I/O"
        );
        eprintln!(
            "{:#?}: formal CLI apply/replay receipt {}",
            self.blocker, receipt["id"]
        );
        receipt
    }

    async fn cli(
        &self,
        declaration: &std::path::Path,
        confirmation: Option<&std::path::Path>,
    ) -> Value {
        let mut command = Command::new(env!("CARGO_BIN_EXE_ipfs-s3-gateway"));
        command
            .kill_on_drop(true)
            .arg("--pinning-migrate-legacy")
            .arg(declaration)
            .env_clear()
            .env("IPFS_S3_CONFIG", &self.config_file)
            .env("RUSTUP_AUTO_INSTALL", "0");
        for name in ["TEMP", "TMP", "TMPDIR", "SystemRoot"] {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        if let Some(confirmation) = confirmation {
            command.arg("--apply").arg(confirmation);
        }
        let output = tokio::time::timeout(Duration::from_secs(40), command.output())
            .await
            .expect("bounded formal migration CLI")
            .unwrap();
        assert!(
            output.status.success(),
            "formal CLI blocked: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }
}
