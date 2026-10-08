use std::{fs, io::ErrorKind, net::TcpListener, path::PathBuf, process::Command};

use ipfs_s3_gateway::{
    pinning::identity::{CleanupMode, ProviderIdentity},
    store::{self, pinning::ledger},
};
use sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement};
use serde_json::json;

pub struct Fixture {
    pub directory: tempfile::TempDir,
    pub database: PathBuf,
    pub config: PathBuf,
    pub declaration: PathBuf,
    pub confirmation: PathBuf,
    pub target: String,
    listener: TcpListener,
}

async fn seed_database(url: &str) -> ProviderIdentity {
    let db = Database::connect(url).await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let identity = ProviderIdentity {
        provider_id: "cli-fixture".into(),
        display_name: "fixture".into(),
        backend: "noop".into(),
        scope: "fixture-account".into(),
        storage_domain: "fixture".into(),
        credential_revision: 1,
        endpoint_revision: 1,
        secret_ref: None,
        api_profile: "noop".into(),
        strategy: "cid".into(),
        retired: false,
        cleanup: CleanupMode::Retain,
    };
    ledger::register_route(&db, &identity.allocation_key(), &identity)
        .await
        .unwrap();
    db.execute_unprepared(
        "INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) VALUES ('legacy-cli',97,3)",
    )
    .await
    .unwrap();
    db.close().await.unwrap();
    identity
}

impl Fixture {
    pub async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let database = directory.path().join("existing.sqlite");
        let url = format!(
            "sqlite://{}?mode=rwc",
            database.display().to_string().replace('\\', "/")
        );
        let identity = seed_database(&url).await;
        let config = directory.path().join("gateway.toml");
        fs::write(
            &config,
            format!(
                "[storage]\ndatabase_url = '{url}'\n[kubo]\nrpc_url = '{endpoint}'\n[[pinning.providers]]\nname = 'unavailable-provider'\nkind = 'pinata'\ntoken_env = 'MIGRATION_MISSING_TOKEN'\nendpoint = '{endpoint}'\npriority = 1\nmax_bytes = 1000\nmax_pins = 10\n"
            ),
        )
        .unwrap();
        let fixture = Self {
            database,
            config,
            declaration: directory.path().join("declaration.json"),
            confirmation: directory.path().join("private-confirmation.json"),
            target: identity.allocation_key(),
            directory,
            listener,
        };
        fixture.write_declaration(&identity);
        fixture
    }

    fn write_declaration(&self, identity: &ProviderIdentity) {
        fs::write(
            &self.declaration,
            serde_json::to_vec(&json!({
                "version": 1, "database_scope": "owned-cli-fixture", "ttl_seconds": 300,
                "confirmation_file": self.confirmation,
                "maintenance": { "all_writers_stopped": true, "all_workers_drained": true },
                "namespaces": [{
                    "source": "legacy-cli", "target": identity.allocation_key(),
                    "backend": "noop", "scope": "fixture-account",
                    "resource_type": identity.resource_type(), "binding_only": false,
                    "target_route": identity.route_snapshot(),
                    "bindings": [{
                        "table": "pin_provider_usage", "key": { "provider": "legacy-cli" },
                        "provider": "legacy-cli", "historical_route": null
                    }]
                }]
            }))
            .unwrap(),
        )
        .unwrap();
    }

    pub fn migrate(&self) -> Command {
        super::command(&self.config, &self.declaration)
    }

    pub fn rollback(&self, receipt: &str) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_ipfs-s3-gateway"));
        command
            .arg("--rollback")
            .arg(receipt)
            .arg(&self.confirmation)
            .env_clear()
            .env("IPFS_S3_CONFIG", &self.config);
        command
    }

    pub fn assert_no_remote_io(&self) {
        assert_eq!(
            self.listener.accept().unwrap_err().kind(),
            ErrorKind::WouldBlock
        );
    }

    pub async fn assert_usage(&self, provider: &str, expected: Option<(i64, i64)>) {
        let url = format!(
            "sqlite://{}?mode=ro",
            self.database.display().to_string().replace('\\', "/")
        );
        let db = Database::connect(url).await.unwrap();
        let row = db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "SELECT reserved_bytes,reserved_pins FROM pin_provider_usage WHERE provider=?",
                [provider.into()],
            ))
            .await
            .unwrap();
        let actual = row.map(|row| {
            (
                row.try_get::<i64>("", "reserved_bytes").unwrap(),
                row.try_get::<i64>("", "reserved_pins").unwrap(),
            )
        });
        assert_eq!(actual, expected);
        db.close().await.unwrap();
    }
}
