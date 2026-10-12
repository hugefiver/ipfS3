#[path = "stage7_management.rs"]
mod publication;

use std::{path::{Path, PathBuf}, process::Output, time::Duration};

use ipfs_s3_gateway::{
    pinning::management::{local_config, ExactObject, PlanRequest},
    store::{self, pinning::management::native},
};
use sea_orm::{ConnectionTrait, DatabaseConnection, Statement};
use serde_json::{Value, json};
use tokio::process::Command;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::{method, path, query_param}};

pub const PRIVATE_METADATA: &str = "STAGE7_CLI_PRIVATE_METADATA_DO_NOT_EXPORT";
pub use publication::NODE;

pub struct Fixture {
    pub directory: tempfile::TempDir,
    pub database: PathBuf,
    pub config_path: PathBuf,
    pub db: DatabaseConnection,
    pub server: MockServer,
    pub config: local_config::ManagementConfig,
    pub identity: ExactObject,
    pub request: PlanRequest,
}

pub fn command(config: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ipfs-s3-gateway"));
    command.args(args).env_clear().env("IPFS_S3_CONFIG", config).kill_on_drop(true);
    command
}

pub async fn output(command: &mut Command) -> Output {
    tokio::time::timeout(Duration::from_secs(60), command.output())
        .await.expect("management CLI exceeded its process budget").unwrap()
}

pub fn report(output: &Output) -> Value {
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    serde_json::from_slice(&output.stdout).unwrap()
}

pub fn write_json(path: &Path, value: &impl serde::Serialize) {
    std::fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
}

impl Fixture {
    pub async fn new(backfill: bool) -> Self {
        let server = MockServer::start().await;
        Mock::given(method("POST")).and(path("/api/v0/id"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ID": publication::NODE})))
            .mount(&server).await;
        Mock::given(method("POST")).and(path("/api/v0/cat")).and(query_param("offline", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"0123456789"))
            .mount(&server).await;
        let (directory, db) = publication::database().await;
        let database = directory.path().join("management.sqlite");
        let config_path = directory.path().join("gateway.toml");
        let url = format!("sqlite://{}?mode=rwc", database.to_string_lossy().replace('\\', "/"));
        let contents = format!(r#"
            [storage]
            database_url = '{url}'
            [kubo]
            rpc_url = '{}'
            [auth]
            credentials = [{{ access_key = 'cli-admin', secret_key = 'CLI_SECRET_DO_NOT_PRINT' }}]
            [decompress_zip]
            auto_pin_outputs = false
            [[pinning.providers]]
            name = 'fixture'
            kind = 'noop'
            priority = 1
            max_bytes = 1000000
            max_pins = 1000
            [[pinning.policies]]
            bucket = 'bucket'
            prefix = ''
            trigger = 'request'
            provider_mode = 'all'
            providers = ['fixture']
            default_duration = '1h'
            max_duration = '2h'
            allow_decompressed = true
            [pinning_identity]
            primary_storage_domain = 'hot'
            [[pinning_identity.providers]]
            config_name = 'fixture'
            provider_id = 'fixture-provider'
            display_name = 'Fixture'
            backend = 'noop'
            scope = 'isolated'
            storage_domain = 'remote'
            credential_revision = 1
            endpoint_revision = 1
            api_profile = 'noop'
            strategy = 'cid'
            cleanup = 'retain'
        "#, server.uri());
        std::fs::write(&config_path, &contents).unwrap();
        let mut raw = publication::config(&server.uri());
        raw.storage.database_url = url;
        raw.auth.credentials[0].access_key = "cli-admin".into();
        raw.auth.credentials[0].secret_key = "CLI_SECRET_DO_NOT_PRINT".into();
        let config = publication::management_config(&raw);
        publication::setup(&db, &config).await;
        let identity = publication::publish(&db, &config, "exact", false).await;
        publication::write_current_decision(&db, &config, &identity).await;
        db.execute(Statement::from_sql_and_values(db.get_database_backend(),
            "UPDATE objects SET metadata=$1 WHERE id=$2",
            [json!({"private": PRIVATE_METADATA}).to_string().into(), identity.object_id.clone().into()]))
            .await.unwrap();
        let request = publication::request(identity.clone(), &config, backfill);
        Self { directory, database, config_path, db, server, config, identity, request }
    }

    pub fn file(&self, name: &str) -> PathBuf { self.directory.path().join(name) }

    pub fn request_file(&self) -> PathBuf {
        let path = self.file("request.json");
        write_json(&path, &self.request);
        path
    }

    pub fn prepare_file(&self, directory: bool) -> PathBuf {
        let path = self.file("prepare.json");
        write_json(&path, &json!({
            "record_id": "explicit-cli-proof", "identity": self.identity,
            "expected_node": publication::NODE, "max_stored_bytes": 10,
            "ttl_seconds": 3600, "directory": directory,
        }));
        path
    }

    pub fn command(&self, args: &[&str]) -> Command { command(&self.config_path, args) }

    pub async fn prepare(&self) -> Value {
        let file = self.prepare_file(false);
        report(&output(&mut self.command(&["--pinning-manage", "--prepare", file.to_str().unwrap()])).await)
    }

    pub async fn confirm(&self, output_path: &Path, ttl: &str, acknowledge: bool) -> Output {
        let request = self.request_file();
        let mut args = vec!["--pinning-manage", request.to_str().unwrap(), "--source-node", publication::NODE,
            "--confirm", output_path.to_str().unwrap(), "--ttl-seconds", ttl];
        if acknowledge { args.extend(["--ack-item", "object"]); }
        output(&mut self.command(&args)).await
    }

    pub async fn import(&self, mode: &str, file: &Path, run: bool) -> Output {
        let mut args = vec!["--pinning-manage", mode, file.to_str().unwrap()];
        if run { args.push("--run"); }
        output(&mut self.command(&args)).await
    }

    pub async fn frame(&self) -> native::NativeFrame { native::capture(&self.db).await.unwrap() }

    pub async fn remote_calls(&self) -> usize { self.server.received_requests().await.unwrap().len() }

    pub fn assert_redacted(&self, output: &Output) {
        let text = format!("{}{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        let endpoint = self.server.uri();
        for secret in [PRIVATE_METADATA, "CLI_SECRET_DO_NOT_PRINT", endpoint.as_str(), self.database.to_str().unwrap()] {
            assert!(!text.contains(secret), "CLI leaked private diagnostic material");
        }
    }

    pub async fn wait_for_confirmation_expiry(&self) {
        let row = self.db.query_one(Statement::from_sql_and_values(self.db.get_database_backend(),
            "SELECT expires_at FROM pin_management_records WHERE plan_id=$1 AND kind='action'",
            [self.request.plan_id.clone().into()])).await.unwrap().unwrap();
        let expiry: chrono::DateTime<chrono::Utc> = row.try_get("", "expires_at").unwrap();
        let now = store::database_clock::database_now(&self.db).await.unwrap();
        if let Ok(delay) = (expiry - now).to_std() { tokio::time::sleep(delay + Duration::from_millis(100)).await; }
    }

    pub async fn unrelated_queue(&self) -> Vec<store::entities::pin_job::Model> {
        use sea_orm::EntityTrait;
        let other = publication::publish_cid(&self.db, &self.config, "old-queue", "QmYwAPJzv5CZsnAzt8auVZRnGiRAz1F7TaqzM9zECsFcdX", true).await;
        assert_ne!(other.object_id, self.identity.object_id);
        store::entities::pin_job::Entity::find().all(&self.db).await.unwrap()
    }

    pub async fn replace_object(&self) {
        let replacement = publication::publish_cid(&self.db, &self.config, &self.identity.key,
            "QmYwAPJzv5CZsnAzt8auVZRnGiRAz1F7TaqzM9zECsFcdX", false).await;
        assert_ne!(replacement.object_id, self.identity.object_id);
        assert_ne!(replacement.private_version_id, self.identity.private_version_id);
        assert_ne!(replacement.cid, self.identity.cid);
    }

    pub async fn publish_directory(&mut self, cid: &str) {
        self.identity = publication::publish_cid(&self.db, &self.config, "directory", cid, false).await;
        publication::write_current_decision(&self.db, &self.config, &self.identity).await;
        self.request = publication::request(self.identity.clone(), &self.config, false);
    }
}
