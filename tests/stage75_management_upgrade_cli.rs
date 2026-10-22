//! CLI routing/authorization boundary only; migration internals are covered by A.
use ipfs_s3_gateway::store::{
    self,
    initialization::{self, SchemaState},
    pinning::management::{native, schema},
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement,
};
use sea_orm_migration::{MigrationTrait, MigratorTrait};
use std::{
    path::{Path, PathBuf},
    process::Output,
    time::Duration,
};
use tokio::process::Command;

// Build the supported historical schema using its actual public migrations,
// without exporting or modifying the production migrator just for this test.
struct Stage7;
impl MigratorTrait for Stage7 {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        use store::migrations::*;
        vec![
            Box::new(m20250701_000001_init::Migration),
            Box::new(m20260707_000001_decompress_zip::Migration),
            Box::new(m20260720_000001_sse_c_key_fingerprint::Migration),
            Box::new(m20260721_000001_multi_provider_pinning::Migration),
            Box::new(m20260729_000001_ipfs3_import::Migration),
            Box::new(m20260729_000002_postgres_utc_timestamps::Migration),
            Box::new(m20260730_000001_standard_mutation_fence::Migration),
            Box::new(m20260813_000001_postgres_json_columns::Migration),
            Box::new(m20260825_000001_object_versioning::Migration),
            Box::new(m20260826_000001_lifecycle_expiration::Migration),
            Box::new(m20260831_000001_bucket_cors::Migration),
            Box::new(m20260901_000001_lifecycle_abort_multipart::Migration),
            Box::new(m20260912_000001_residency_references::Migration),
            Box::new(m20260912_000002_lifecycle_transition::Migration),
            Box::new(m20260919_000001_standard_mutation_lease::Migration),
            Box::new(m20260920_000001_pin_submit_history::Migration),
            Box::new(m20260920_000002_pin_identity_ledger::Migration),
            Box::new(m20260920_000003_pin_extension_decision::Migration),
            Box::new(m20260920_000004_multipart_pin_decision::Migration),
            Box::new(m20260920_000005_import_pin_decision::Migration),
            Box::new(m20260920_000006_pin_submit_correlation::Migration),
            Box::new(m20260927_000001_zip_batches::Migration),
            Box::new(m20260927_000002_import_zip_root_capture::Migration),
            Box::new(m20260927_000003_zip_mpu_replay::Migration),
            Box::new(m20260927_000004_zip_v2_execution::Migration),
            Box::new(m20260927_000005_zip_v2_import::Migration),
            Box::new(m20260927_000006_zip_v2_mpu_intake::Migration),
            Box::new(m20260927_000007_zip_v2_mpu_completion::Migration),
            Box::new(m20261004_000001_rpc_submission_ledger::Migration),
            Box::new(m20261004_000002_cluster_replica_contract::Migration),
            Box::new(m20261007_000001_legacy_pinning_migration::Migration),
            Box::new(m20261008_000001_zip_intent_capture::Migration),
            Box::new(m20261012_000001_pinning_management::Migration),
            Box::new(m20261013_000001_zip_root_logical_release::Migration),
        ]
    }
}

fn sqlite_url(path: &Path) -> String {
    format!(
        "sqlite://{}?mode=rwc",
        path.to_string_lossy().replace('\\', "/")
    )
}

fn command(directory: &Path, args: &[String]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ipfs-s3-gateway"));
    command.current_dir(directory).args(args).kill_on_drop(true);
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("IPFS_S3_") {
            command.env_remove(name);
        }
    }
    // Upgrade must not load gateway config, even when it is unusable.
    command.env("IPFS_S3_CONFIG", directory.join("invalid.toml"));
    command.env("IPFS_S3_MASTER_KEY", "not-a-key");
    command.env("IPFS_S3_BIND", "not-a-bind-address");
    command
}

async fn output(mut command: Command) -> Output {
    tokio::time::timeout(Duration::from_secs(45), command.output())
        .await
        .expect("local CLI must exit without starting a gateway/worker")
        .unwrap()
}

fn args(url: &str, identity: &str, scope: &str, source: &str) -> Vec<String> {
    [
        "--pinning-manage-upgrade",
        "--database-url",
        url,
        "--native-identity",
        identity,
        "--database-scope",
        scope,
        "--source-version",
        source,
        "--target-version",
        schema::VERSION,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

struct Fixture {
    directory: tempfile::TempDir,
    db: DatabaseConnection,
    url: String,
    identity: String,
    scope: String,
    source: String,
}

#[derive(PartialEq, Eq)]
struct Before {
    catalog: Vec<(String, String, String, Option<String>)>,
    versions: Vec<String>,
    scope: (String, String),
    buckets: Vec<String>,
}

impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(
            directory.path().join("invalid.toml"),
            "PRIVATE_UNUSABLE_CONFIG = [",
        )
        .unwrap();
        let url = sqlite_url(&directory.path().join("selected.sqlite"));
        let mut options = ConnectOptions::new(url.clone());
        options
            .max_connections(1)
            .min_connections(1)
            .sqlx_logging(false);
        let db = Database::connect(options).await.unwrap();
        let source = Stage7::migrations().last().unwrap().name().to_owned();
        Stage7::up(&db, None).await.unwrap();
        store::bucket::create(&db, "upgrade-cli-sentinel", None)
            .await
            .unwrap();
        let (scope, identity) = native::scope::binding(&db).await.unwrap();
        assert_eq!(
            initialization::classify(&db).await.unwrap(),
            SchemaState::UpgradeRequired
        );
        Self {
            directory,
            db,
            url,
            identity,
            scope,
            source,
        }
    }
    fn args(&self) -> Vec<String> {
        args(&self.url, &self.identity, &self.scope, &self.source)
    }
    async fn before(&self) -> Before {
        let rows = self
            .db
            .query_all(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name",
            ))
            .await
            .unwrap();
        let catalog = rows
            .iter()
            .map(|r| {
                (
                    r.try_get("", "type").unwrap(),
                    r.try_get("", "name").unwrap(),
                    r.try_get("", "tbl_name").unwrap(),
                    r.try_get("", "sql").unwrap(),
                )
            })
            .collect();
        let versions = self
            .db
            .query_all(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT version FROM seaql_migrations ORDER BY version",
            ))
            .await
            .unwrap()
            .iter()
            .map(|r| r.try_get("", "version").unwrap())
            .collect();
        let buckets = self
            .db
            .query_all(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT name FROM buckets ORDER BY name",
            ))
            .await
            .unwrap()
            .iter()
            .map(|r| r.try_get("", "name").unwrap())
            .collect();
        Before {
            catalog,
            versions,
            buckets,
            scope: native::scope::binding(&self.db).await.unwrap(),
        }
    }
    async fn close(self) {
        self.db.close().await.unwrap();
        self.directory.close().unwrap();
    }
}

#[tokio::test]
async fn rejected_upgrade_inputs_never_create_database() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("invalid.toml"),
        "PRIVATE_UNUSABLE_CONFIG = [",
    )
    .unwrap();
    let missing = directory.path().join("must-not-create.sqlite");
    let all = args(
        &sqlite_url(&missing),
        "PRIVATE_NATIVE_IDENTITY",
        "PRIVATE_DATABASE_SCOPE",
        "original-version",
    );
    let mut cases = vec![vec!["--pinning-manage-upgrade".into()]];
    for pair in 0..5 {
        let mut incomplete = all.clone();
        incomplete.drain(1 + pair * 2..3 + pair * 2);
        cases.push(incomplete);
        let mut duplicate = all.clone();
        duplicate.extend_from_slice(&all[1 + pair * 2..3 + pair * 2]);
        cases.push(duplicate);
        let mut replaced = all.clone();
        replaced[1 + pair * 2] = all[if pair == 0 { 3 } else { 1 }].clone();
        cases.push(replaced);
    }
    let mut unknown = all.clone();
    unknown[1] = "--unknown".into();
    cases.push(unknown);
    for mode in [
        "--pinning-manage",
        "--pinning-migrate-legacy",
        "--ready-probe",
        "--pinning-doctor",
    ] {
        let mut mixed = vec![mode.into()];
        mixed.extend(all.clone());
        cases.push(mixed);
    }
    cases.push(all);
    for args in cases {
        let result = output(command(directory.path(), &args)).await;
        assert!(!result.status.success());
        assert!(!missing.exists());
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        for private in [
            "PRIVATE_UNUSABLE_CONFIG",
            "PRIVATE_NATIVE_IDENTITY",
            "PRIVATE_DATABASE_SCOPE",
            missing.to_str().unwrap(),
        ] {
            assert!(!text.contains(private));
        }
    }
}

#[tokio::test]
async fn wrong_bindings_and_ordinary_startup_do_not_upgrade_an_old_database() {
    let fixture = Fixture::new().await;
    let before = fixture.before().await;
    for value_index in [4, 6, 8, 10] {
        let mut args = fixture.args();
        args[value_index] = "incorrect-binding".into();
        let result = output(command(fixture.directory.path(), &args)).await;
        assert!(!result.status.success());
        assert!(fixture.before().await == before);
    }
    let config: PathBuf = fixture.directory.path().join("valid.toml");
    std::fs::write(&config, "").unwrap();
    let mut startup = command(fixture.directory.path(), &[]);
    startup
        .env("IPFS_S3_CONFIG", config)
        .env("IPFS_S3_DATABASE_URL", &fixture.url)
        .env("IPFS_S3_MASTER_KEY", "7".repeat(64))
        .env("IPFS_S3_BIND", "127.0.0.1:0")
        .env("IPFS_S3_KUBO_RPC_URL", "http://127.0.0.1:1");
    assert!(!output(startup).await.status.success());
    assert!(fixture.before().await == before);
    assert_eq!(
        initialization::classify(&fixture.db).await.unwrap(),
        SchemaState::UpgradeRequired
    );
    fixture.close().await;
}

#[tokio::test]
async fn explicit_upgrade_runs_independently_of_gateway_config_on_only_named_database() {
    let fixture = Fixture::new().await;
    let other = fixture.directory.path().join("unselected.sqlite");
    let mut upgrade = command(fixture.directory.path(), &fixture.args());
    upgrade.env("IPFS_S3_DATABASE_URL", sqlite_url(&other));
    let result = output(upgrade).await;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["mode"], "management_upgrade");
    assert_eq!(report["upgraded"], true);
    assert_eq!(report["source_version"], fixture.source);
    assert_eq!(report["target_version"], schema::VERSION);
    assert_eq!(report["source_rpc"], "not_performed");
    assert_eq!(report["provider_rpc"], "not_performed");
    assert!(!other.exists());
    assert_eq!(
        initialization::classify(&fixture.db).await.unwrap(),
        SchemaState::Current
    );
    assert_eq!(
        native::scope::binding(&fixture.db).await.unwrap(),
        (fixture.scope.clone(), fixture.identity.clone())
    );
    let bucket = fixture
        .db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT name FROM buckets WHERE name='upgrade-cli-sentinel'",
        ))
        .await
        .unwrap();
    assert!(bucket.is_some());
    fixture.close().await;
}
