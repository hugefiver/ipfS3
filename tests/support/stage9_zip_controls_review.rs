//! Real authenticated ingress fixture. Only local Noop policy; no workers run.
use std::{collections::BTreeMap, sync::Arc};

use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{
    config::Config,
    import::{
        downloader::{SourceDownloader, StrictPublicAddressPolicy},
        pipeline::ImportCoordinator,
    },
    state::AppState,
    store,
};
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

use crate::support::{
    decompress::{
        S3ServerHandle, S3TestEndpoint, legal_single_entry_zip, start_s3_server_with_imports,
    },
    sigv4::send_sigv4,
    stage9_zip_replay_source::{ORIGIN, Source, URL},
};

pub const BUCKET: &str = "test-bkt";
pub const KEY: &str = "archive.zip";
pub const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

pub struct Fixture {
    pub state: Arc<AppState>,
    pub kubo: MockServer,
    pub source: Arc<Source>,
    server: S3ServerHandle,
    directory: tempfile::TempDir,
}

impl S3TestEndpoint for Fixture {
    fn endpoint(&self) -> &str {
        &self.server.endpoint
    }
    fn bucket(&self) -> &str {
        BUCKET
    }
}

impl Fixture {
    pub async fn new() -> Self {
        let archive = legal_single_entry_zip();
        let kubo = MockServer::start().await;
        for (rpc, response) in [
            (
                "/api/v0/add",
                format!("{{\"Hash\":\"{CID}\",\"Size\":\"{}\"}}\n", archive.len()),
            ),
            ("/api/v0/pin/add", format!("{{\"Pins\":[\"{CID}\"]}}\n")),
        ] {
            Mock::given(method("POST"))
                .and(path(rpc))
                .respond_with(ResponseTemplate::new(200).set_body_string(response))
                .mount(&kubo)
                .await;
        }
        let directory = tempfile::Builder::new()
            .prefix("stage9-controls-review-")
            .tempdir()
            .unwrap();
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            directory
                .path()
                .join("gateway.sqlite")
                .display()
                .to_string()
                .replace('\\', "/")
        );
        let config: Config = toml::from_str(&format!(
            r#"
[kubo]
rpc_url = {kubo_url:?}
[storage]
database_url = {database_url:?}
[[auth.credentials]]
access_key = 'test'
secret_key = 'test'
[decompress_zip]
unixfs_directory_root = false
auto_pin_outputs = true
[imports]
allowed_https_origins = [{ORIGIN:?}]
[pinning_identity]
primary_storage_domain = 'controls-review-kubo'
[[pinning_identity.providers]]
config_name = 'local-noop'
provider_id = 'controls-review-noop'
display_name = 'Controls review Noop'
backend = 'noop'
scope = 'controls-review-scope'
storage_domain = 'controls-review-domain'
credential_revision = 1
endpoint_revision = 1
api_profile = 'noop'
strategy = 'cid'
cleanup = 'retain'
[[pinning.providers]]
name = 'local-noop'
kind = 'noop'
priority = 1
max_bytes = 1000000
max_pins = 100
[[pinning.policies]]
bucket = 'test-bkt'
prefix = ''
trigger = 'always'
provider_mode = 'one'
providers = ['local-noop']
default_duration = '1h'
max_duration = '1d'
allow_decompressed = true
"#,
            kubo_url = kubo.uri()
        ))
        .unwrap();
        let state = AppState::new(&config).await.unwrap();
        assert_eq!(
            state
                .pinning
                .provider(
                    &state.pinning.effective_config().providers[0]
                        .identity
                        .allocation_key()
                )
                .unwrap()
                .invocation_route(),
            ("noop", "cid")
        );
        store::bucket::create(state.store.db(), BUCKET, Some("test"))
            .await
            .unwrap();
        let source = Arc::new(Source::new(&archive));
        let validated = config.imports.validate().unwrap();
        let downloader = SourceDownloader::with_components(
            Arc::new(validated.clone()),
            source.clone(),
            Arc::new(StrictPublicAddressPolicy),
            source.clone(),
        );
        let imports = ImportCoordinator::new(validated, downloader);
        let server = start_s3_server_with_imports(state.clone(), Arc::default(), imports).await;
        Self {
            state,
            kubo,
            source,
            server,
            directory,
        }
    }

    pub async fn shutdown(self) {
        self.server.shutdown().await;
        self.state.store.db().clone().close().await.unwrap();
        self.directory.close().unwrap();
    }

    pub async fn request(
        &self,
        entry: Entry,
        bucket: &str,
        query: &[(&str, &str)],
        mut headers: HeaderMap,
    ) -> (StatusCode, String) {
        let (method, body) = match entry {
            Entry::Ordinary => (reqwest::Method::PUT, b"new content".to_vec()),
            Entry::ZipPut => (reqwest::Method::PUT, legal_single_entry_zip()),
            Entry::Mpu => (reqwest::Method::POST, Vec::new()),
            Entry::Import => {
                headers.insert(
                    http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/xml"),
                );
                (
                    reqwest::Method::POST,
                    format!("<IPFS3ImportRequest><URL>{URL}</URL></IPFS3ImportRequest>")
                        .into_bytes(),
                )
            }
        };
        let response = send_sigv4(
            method,
            self.endpoint(),
            bucket,
            KEY,
            query,
            body,
            headers,
            "test",
        )
        .await;
        (response.status(), response.text().await.unwrap())
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Entry {
    Ordinary,
    ZipPut,
    Mpu,
    Import,
}

impl Entry {
    pub fn query(self) -> Vec<(&'static str, &'static str)> {
        match self {
            Self::Ordinary => vec![],
            Self::ZipPut => vec![("decompress-zip", "out/")],
            Self::Mpu => vec![("uploads", ""), ("decompress-zip", "out/")],
            Self::Import => vec![("ipfs3-import", ""), ("decompress-zip", "out/")],
        }
    }
}

pub fn tagging(value: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-tagging", HeaderValue::from_str(value).unwrap());
    headers
}

/// Entire SQLite state, including object metadata, decisions, mutations, ZIP
/// intake, jobs, quota and native leases. SQL quote() also preserves NULL/BLOB.
pub async fn snapshot(db: &DatabaseConnection) -> BTreeMap<String, Vec<String>> {
    let tables = db
        .query_all(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
        ))
        .await
        .unwrap();
    let mut snapshot = BTreeMap::new();
    for table in tables {
        let name: String = table.try_get("", "name").unwrap();
        let quoted = format!("\"{}\"", name.replace('"', "\"\""));
        let columns = db
            .query_all(Statement::from_string(
                DbBackend::Sqlite,
                format!("PRAGMA table_info({quoted})"),
            ))
            .await
            .unwrap();
        let projection = columns
            .into_iter()
            .map(|column| {
                let column: String = column.try_get("", "name").unwrap();
                format!("quote(\"{}\")", column.replace('"', "\"\""))
            })
            .collect::<Vec<_>>()
            .join(" || '|' || ");
        let rows = db
            .query_all(Statement::from_string(
                DbBackend::Sqlite,
                format!("SELECT {projection} AS row FROM {quoted} ORDER BY row"),
            ))
            .await
            .unwrap();
        snapshot.insert(
            name,
            rows.into_iter()
                .map(|row| row.try_get("", "row").unwrap())
                .collect(),
        );
    }
    snapshot
}

pub async fn rejected_unchanged(
    h: &Fixture,
    entry: Entry,
    bucket: &str,
    query: &[(&str, &str)],
    headers: HeaderMap,
    code: &str,
) {
    let before = snapshot(h.state.store.db()).await;
    let rpc_before = h.kubo.received_requests().await.unwrap().len();
    let source_before = h.source.calls();
    let (status, body) = h.request(entry, bucket, query, headers).await;
    let after = snapshot(h.state.store.db()).await;
    let rpc_after = h.kubo.received_requests().await.unwrap().len();
    let source_after = h.source.calls();
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "{entry:?} {query:?}: {body}; DB unchanged={}; RPC {rpc_before}->{rpc_after}; source {source_before}->{source_after}",
        before == after
    );
    assert!(
        body.contains(&format!("<Code>{code}</Code>")),
        "{entry:?}: {body}"
    );
    assert_eq!(
        after, before,
        "{entry:?}: rejection must not mutate any DB state"
    );
    assert_eq!(
        rpc_after, rpc_before,
        "{entry:?}: no Kubo source add/pin or other RPC"
    );
    assert_eq!(
        source_after, source_before,
        "{entry:?}: no HTTP source requests"
    );
}
