//! Owned real-TCP fixture for legacy ZIP automatic-output pinning acceptance.
//!
//! No pinning worker is started: local PSA endpoints are routing fixtures, not
//! evidence that a remote provider has pinned anything. Assertions observe durable publication.

use std::{collections::HashMap, sync::Arc, time::Duration};

use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{
    config::Config,
    import::{
        downloader::SourceDownloader, pipeline::ImportCoordinator, worker::ImportWorkerHandle,
    },
    state::AppState,
    store::{self, entities::import_job},
};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

use crate::support::{
    decompress::{
        AddReply, KuboScript, S3ServerHandle, S3TestEndpoint, complete_multipart,
        start_kubo_harness, start_s3_server_with_imports,
    },
    sigv4::send_sigv4,
};

pub const ARCHIVE_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
pub const OUTPUT_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyk4";
pub const ROOT_CID: &str = "bafybeib4r26s6xrk5uqyy6k5lkwxmrsfecmfxluvlz44b2qnt3rottiw3u";
pub const PART_CID: &str = "QmStage9Part";
pub const BUCKET: &str = "test-bkt";
pub const ARCHIVE_KEY: &str = "archive.zip";
pub const OUTPUT_BODY: &[u8] = b"\0";

#[derive(Clone, Copy, Debug)]
pub enum EntryPoint {
    Put,
    CompleteMpu,
    Import,
}

pub const ENTRY_POINTS: [EntryPoint; 3] =
    [EntryPoint::Put, EntryPoint::CompleteMpu, EntryPoint::Import];

pub struct Fixture {
    pub state: Arc<AppState>,
    pub kubo: MockServer,
    pub provider: String,
    server: Option<S3ServerHandle>,
    worker: Option<ImportWorkerHandle>,
    shutdown: CancellationToken,
    directory: Option<tempfile::TempDir>,
}

impl S3TestEndpoint for Fixture {
    fn endpoint(&self) -> &str {
        &self.server.as_ref().expect("owned S3 server").endpoint
    }

    fn bucket(&self) -> &str {
        BUCKET
    }
}

impl Fixture {
    pub async fn new(
        entry: EntryPoint,
        auto: bool,
        archive: &[u8],
        replies: Vec<AddReply>,
    ) -> Self {
        assert_ne!(ARCHIVE_CID, OUTPUT_CID);
        for cid in [ARCHIVE_CID, OUTPUT_CID, ROOT_CID] {
            cid.parse::<cid::Cid>().expect("valid test CID");
        }
        let mut add_replies = Vec::new();
        match entry {
            EntryPoint::Put => add_replies.push(AddReply::Ok(ARCHIVE_CID)),
            EntryPoint::CompleteMpu => {
                add_replies.extend([AddReply::Ok(PART_CID), AddReply::Ok(ARCHIVE_CID)])
            }
            EntryPoint::Import => {}
        }
        let mut cat_bodies = HashMap::from([
            (ARCHIVE_CID.into(), archive.to_vec()),
            (PART_CID.into(), archive.to_vec()),
            (OUTPUT_CID.into(), OUTPUT_BODY.to_vec()),
        ]);
        for (index, reply) in replies.iter().enumerate() {
            if let AddReply::Ok(cid) = reply
                && *cid != OUTPUT_CID
            {
                cat_bodies.insert((*cid).to_owned(), (index as u64).to_le_bytes().to_vec());
            }
        }
        let sizes = cat_bodies
            .iter()
            .map(|(cid, bytes)| (cid.clone(), bytes.len()))
            .collect();
        add_replies.extend(replies);
        let kubo = start_kubo_harness(KuboScript {
            add_replies,
            cat_bodies,
        })
        .await
        .server;
        mount_directory_and_import(&kubo, sizes).await;
        let directory = tempfile::Builder::new()
            .prefix("stage9-zip-http-")
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
        // Keep the new option in TOML, not a typed field: baseline serde may
        // ignore it, but it must still satisfy the same on/off behavior checks.
        let text = format!(
            r#"
[kubo]
rpc_url = {kubo_url:?}
[storage]
database_url = {database_url:?}
[[auth.credentials]]
access_key = 'test'
secret_key = 'test'
[decompress_zip]
unixfs_directory_root = true
auto_pin_outputs = {auto}
[pinning_identity]
primary_storage_domain = 'stage9-owned-kubo'
[[pinning_identity.providers]]
config_name = 'pinata-primary'
provider_id = 'stage9-pinata-primary'
display_name = 'Stage9 Pinata primary'
backend = 'pinata'
scope = 'stage9-pinata-primary'
storage_domain = 'stage9-pinata-primary'
credential_revision = 1
endpoint_revision = 1
secret_ref = 'env:PATH'
api_profile = 'pinata-psa'
strategy = 'cid'
cleanup = 'retain'
[[pinning_identity.providers]]
config_name = 'filebase-primary'
provider_id = 'stage9-filebase-primary'
display_name = 'Stage9 Filebase primary'
backend = 'filebase'
scope = 'stage9-filebase-primary'
storage_domain = 'stage9-filebase-primary'
credential_revision = 1
endpoint_revision = 1
secret_ref = 'env:PATH'
api_profile = 'filebase-psa'
strategy = 'cid'
cleanup = 'retain'
[[pinning_identity.providers]]
config_name = 'pinata-fallback'
provider_id = 'stage9-pinata-fallback'
display_name = 'Stage9 Pinata fallback'
backend = 'pinata'
scope = 'stage9-pinata-fallback'
storage_domain = 'stage9-pinata-fallback'
credential_revision = 1
endpoint_revision = 1
secret_ref = 'env:PATH'
api_profile = 'pinata-psa'
strategy = 'cid'
cleanup = 'retain'
[[pinning_identity.providers]]
config_name = 'filebase-fallback'
provider_id = 'stage9-filebase-fallback'
display_name = 'Stage9 Filebase fallback'
backend = 'filebase'
scope = 'stage9-filebase-fallback'
storage_domain = 'stage9-filebase-fallback'
credential_revision = 1
endpoint_revision = 1
secret_ref = 'env:PATH'
api_profile = 'filebase-psa'
strategy = 'cid'
cleanup = 'retain'
[[pinning.providers]]
name = 'pinata-primary'
kind = 'pinata'
token_env = 'PATH'
endpoint = {pinata_endpoint:?}
priority = 1
max_bytes = 1000000
max_pins = 10000
[[pinning.providers]]
name = 'filebase-primary'
kind = 'filebase'
token_env = 'PATH'
endpoint = {filebase_endpoint:?}
priority = 2
max_bytes = 1000000
max_pins = 10000
[[pinning.providers]]
name = 'pinata-fallback'
kind = 'pinata'
token_env = 'PATH'
endpoint = {pinata_endpoint:?}
priority = 3
max_bytes = 1000000
max_pins = 10000
[[pinning.providers]]
name = 'filebase-fallback'
kind = 'filebase'
token_env = 'PATH'
endpoint = {filebase_endpoint:?}
priority = 4
max_bytes = 1000000
max_pins = 10000
[[pinning.policies]]
bucket = 'test-bkt'
prefix = ''
trigger = 'always'
provider_mode = 'one'
providers = ['pinata-primary', 'filebase-primary', 'pinata-fallback', 'filebase-fallback']
default_duration = '1h'
max_duration = '30d'
allow_decompressed = true
"#,
            kubo_url = kubo.uri(),
            pinata_endpoint = format!("{}/psa", kubo.uri()),
            filebase_endpoint = format!("{}/v1/ipfs", kubo.uri()),
        );
        let config: Config = toml::from_str(&text).expect("explicit on/off gateway config");
        let state = AppState::new(&config)
            .await
            .expect("initialize configured gateway and routes");
        let effective = state.pinning.effective_config();
        assert_eq!(effective.providers.len(), 4);
        for configured in &effective.providers {
            let provider = configured.identity.allocation_key();
            assert_eq!(
                state
                    .pinning
                    .provider(&provider)
                    .unwrap()
                    .invocation_route(),
                ("psa", "cid")
            );
        }
        let provider = effective
            .providers
            .iter()
            .min_by_key(|provider| provider.limits.priority)
            .unwrap()
            .identity
            .allocation_key();
        store::bucket::create(state.store.db(), BUCKET, Some("test"))
            .await
            .unwrap();
        let import_config = config.imports.validate().unwrap();
        let downloader = SourceDownloader::production(Arc::new(import_config.clone()));
        let imports = ImportCoordinator::new(import_config, downloader);
        let server =
            start_s3_server_with_imports(state.clone(), Arc::default(), imports.clone()).await;
        let shutdown = CancellationToken::new();
        let worker = matches!(entry, EntryPoint::Import)
            .then(|| imports.start(state.clone(), shutdown.clone()));
        Self {
            state,
            kubo,
            provider,
            server: Some(server),
            worker,
            shutdown,
            directory: Some(directory),
        }
    }

    pub async fn enable_versioning(&self) {
        let response = send_sigv4(
            reqwest::Method::PUT,
            self.endpoint(),
            BUCKET,
            "",
            &[("versioning", "")],
            b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec(),
            HeaderMap::new(),
            "test",
        )
        .await;
        let status = response.status();
        let body = response.text().await.unwrap();
        assert_eq!(status, StatusCode::OK, "signed versioning: {body}");
    }

    pub async fn publish(
        &self,
        entry: EntryPoint,
        archive: Vec<u8>,
        decompress: bool,
        manual: bool,
    ) -> String {
        let options = if decompress {
            vec![("decompress-zip", "expanded/")]
        } else {
            Vec::new()
        };
        let mut headers = HeaderMap::new();
        if manual {
            headers.insert(
                "x-amz-tagging",
                HeaderValue::from_static("ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed"),
            );
        }
        match entry {
            EntryPoint::Put => {
                let response = send_sigv4(
                    reqwest::Method::PUT,
                    self.endpoint(),
                    BUCKET,
                    ARCHIVE_KEY,
                    &options,
                    archive,
                    headers,
                    "test",
                )
                .await;
                assert_eq!(response.status(), StatusCode::OK, "legacy PUT");
                assert_eq!(
                    response.headers()[http::header::ETAG],
                    format!("\"{ARCHIVE_CID}\"")
                );
                let id = if decompress {
                    response.headers()["x-ipfs-s3-zip-batch-id"]
                        .to_str()
                        .unwrap()
                        .to_owned()
                } else {
                    String::new()
                };
                response.bytes().await.unwrap();
                id
            }
            EntryPoint::CompleteMpu => {
                let mut query = vec![("uploads", "")];
                query.extend(options);
                let create = send_sigv4(
                    reqwest::Method::POST,
                    self.endpoint(),
                    BUCKET,
                    ARCHIVE_KEY,
                    &query,
                    Vec::new(),
                    headers,
                    "test",
                )
                .await;
                assert_eq!(create.status(), StatusCode::OK, "legacy CreateMPU");
                let xml = create.text().await.unwrap();
                let id = xml
                    .split("<UploadId>")
                    .nth(1)
                    .unwrap()
                    .split("</UploadId>")
                    .next()
                    .unwrap()
                    .to_owned();
                let part = send_sigv4(
                    reqwest::Method::PUT,
                    self.endpoint(),
                    BUCKET,
                    ARCHIVE_KEY,
                    &[("partNumber", "1"), ("uploadId", &id)],
                    archive,
                    HeaderMap::new(),
                    "test",
                )
                .await;
                assert_eq!(part.status(), StatusCode::OK, "legacy UploadPart");
                let etag = part.headers()[http::header::ETAG]
                    .to_str()
                    .unwrap()
                    .trim_matches('"')
                    .to_owned();
                part.bytes().await.unwrap();
                // Complete carries only the old uploadId/XML inputs, not new ZIP controls.
                let response = complete_multipart(self, ARCHIVE_KEY, &id, &[(1, etag)]).await;
                let status = response.status();
                let xml = response.text().await.unwrap();
                assert_eq!(status, StatusCode::OK, "legacy CompleteMPU: {xml}");
                assert!(
                    xml.contains(if decompress {
                        "<DecompressZipResult>"
                    } else {
                        "<CompleteMultipartUploadResult"
                    }),
                    "legacy response selection: {xml}"
                );
                assert!(xml.contains(ARCHIVE_CID), "{xml}");
                id
            }
            EntryPoint::Import => {
                let mut query = vec![("ipfs3-import", "")];
                query.extend(options);
                headers.insert(
                    http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/xml"),
                );
                let response = send_sigv4(
                    reqwest::Method::POST,
                    self.endpoint(),
                    BUCKET,
                    ARCHIVE_KEY,
                    &query,
                    format!("<IPFS3ImportRequest><CID>{ARCHIVE_CID}</CID></IPFS3ImportRequest>")
                        .into_bytes(),
                    headers,
                    "test",
                )
                .await;
                let status = response.status();
                let id = response
                    .headers()
                    .get("x-ipfs3-import-job-id")
                    .map(|id| id.to_str().unwrap().to_owned());
                let body = response.text().await.unwrap();
                assert_eq!(
                    status,
                    StatusCode::ACCEPTED,
                    "legacy import admission: {body}"
                );
                let id = id.expect("accepted legacy import job ID");
                let job = self.wait_for_import(&id).await;
                assert_eq!(job.state, "completed", "legacy import: {job:?}");
                let response = send_sigv4(
                    reqwest::Method::GET,
                    self.endpoint(),
                    BUCKET,
                    ARCHIVE_KEY,
                    &[("ipfs3-import", &id)],
                    Vec::new(),
                    HeaderMap::new(),
                    "test",
                )
                .await;
                assert_eq!(response.status(), StatusCode::OK, "legacy import status");
                let xml = response.text().await.unwrap();
                assert!(xml.contains("<State>completed</State>"), "{xml}");
                id
            }
        }
    }

    async fn wait_for_import(&self, id: &str) -> import_job::Model {
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let job = import_job::Entity::find_by_id(id)
                    .one(self.state.store.db())
                    .await
                    .unwrap()
                    .unwrap();
                if matches!(job.state.as_str(), "completed" | "failed" | "cancelled") {
                    return job;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        match result {
            Ok(job) => job,
            Err(error) => {
                let job = import_job::Entity::find_by_id(id)
                    .one(self.state.store.db())
                    .await
                    .unwrap();
                let requests = self.kubo.received_requests().await.unwrap();
                panic!(
                    "legacy import reaches terminal state with default worker settings: {error}; job={job:?}; rpc={:?}",
                    requests
                        .iter()
                        .map(|request| request.url.as_str())
                        .collect::<Vec<_>>()
                );
            }
        }
    }

    pub async fn shutdown(mut self) {
        self.shutdown.cancel();
        if let Some(worker) = self.worker.take() {
            worker.shutdown(Duration::from_secs(2)).await;
        }
        self.server.take().unwrap().shutdown().await;
        self.state.store.db().clone().close().await.unwrap();
        self.directory
            .take()
            .unwrap()
            .close()
            .expect("remove only owned test database directory");
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

async fn mount_directory_and_import(kubo: &MockServer, mut sizes: HashMap<String, usize>) {
    let archive_size = sizes[ARCHIVE_CID];
    sizes.insert(ROOT_CID.to_owned(), 1);
    Mock::given(method("POST")).and(path("/api/v0/files/stat"))
        .respond_with(move |request: &wiremock::Request| {
            let arg = request.url.query_pairs().find(|(key, _)| key == "arg").unwrap().1.into_owned();
            let cid = arg.strip_prefix("/ipfs/").unwrap();
            let size = sizes[cid];
            ResponseTemplate::new(200).set_body_string(format!(
                "{{\"Hash\":\"{cid}\",\"Size\":{size},\"CumulativeSize\":{size},\"Type\":\"file\",\"WithLocality\":true,\"Local\":true}}"))
        }).mount(kubo).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/put"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Cid\":{{\"/\":\"{ROOT_CID}\"}}}}")),
        )
        .mount(kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/resolve"))
        .respond_with(|request: &wiremock::Request| {
            let arg = request
                .url
                .query_pairs()
                .find(|(key, _)| key == "arg")
                .unwrap()
                .1
                .into_owned();
            let path = arg.rsplit('/').next().unwrap();
            let cid = path
                .strip_prefix("entry-")
                .and_then(|path| path.strip_suffix(".txt"))
                .and_then(|index| index.parse::<usize>().ok())
                .map(crate::zip_fixture::distinct_output_cid)
                .unwrap_or_else(|| OUTPUT_CID.to_owned());
            ResponseTemplate::new(200).set_body_string(format!("{{\"Path\":\"/ipfs/{cid}\"}}"))
        })
        .mount(kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("{\"ID\":\"QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE\"}"),
        )
        .mount(kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/ls"))
        .respond_with(|request: &wiremock::Request| {
            let arg = request
                .url
                .query_pairs()
                .find(|(key, _)| key == "arg")
                .unwrap()
                .1
                .into_owned();
            ResponseTemplate::new(200).set_body_string(format!(
                "{{\"Keys\":{{\"{arg}\":{{\"Type\":\"recursive\"}}}}}}"
            ))
        })
        .mount(kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("arg", ROOT_CID))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{ROOT_CID}\"]}}")),
        )
        .with_priority(1)
        .mount(kubo)
        .await;
    // Legacy CID imports consume the streaming recursive-pin terminal record.
    // The generic upload mock's empty Pins list is not a successful import pin.
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("arg", ARCHIVE_CID))
        .and(query_param("progress", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "{{\"Progress\":1,\"Bytes\":{archive_size}}}\n{{\"Pins\":[\"{ARCHIVE_CID}\"]}}\n"
        )))
        .with_priority(1)
        .mount(kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/routing/findprovs"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("{\"Type\":4,\"Responses\":[{\"ID\":\"mock-local-source\"}]}\n"),
        )
        .mount(kubo)
        .await;
}
