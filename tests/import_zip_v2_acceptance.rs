//! Authenticated intake, durable replay and read-only status; no v2 worker runs here.
#[allow(dead_code)]
mod support;

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::import::{
    ImportConfig,
    downloader::{
        AuthorizedSource, DownloadError, DownloadLimits, DownloadStream, ImportHttpTransport,
        ImportResolver, SourceDownloader, StrictPublicAddressPolicy,
    },
    pipeline::ImportCoordinator,
};
use ipfs_s3_gateway::store::{
    entities::{
        import_destination, import_job, import_prefix_claim, object, object_version,
        standard_mutation_lease,
    },
    zip::{execution, import_intake},
};
use sea_orm::{ConnectionTrait, EntityTrait, PaginatorTrait};
use support::{
    cors::S3ServerHandle,
    decompress::{S3TestEndpoint, start_s3_server_with_imports},
    import::post_import,
};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

// Intake is intentionally tested independently of worker scheduling. Source IO
// is measured at the real downloader transport seam, never a fabricated list.
#[derive(Default)]
struct IntakeSource {
    calls: AtomicUsize,
}

impl IntakeSource {
    fn url(&self, path: &str) -> String {
        format!("https://intake.example.test{path}")
    }
}

#[async_trait::async_trait]
impl ImportResolver for IntakeSource {
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, DownloadError> {
        if host != "intake.example.test" {
            return Err(DownloadError::Dns);
        }
        Ok(vec![SocketAddr::from(([1, 1, 1, 1], port))])
    }
}

#[async_trait::async_trait]
impl ImportHttpTransport for IntakeSource {
    async fn open(
        &self,
        _source: AuthorizedSource,
        _limits: DownloadLimits,
        _progress: tokio::sync::watch::Sender<u64>,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> Result<DownloadStream, DownloadError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(DownloadError::Connect)
    }
}

struct IntakeHarness {
    endpoint: String,
    bucket: String,
    state: Arc<ipfs_s3_gateway::state::AppState>,
    source: Arc<IntakeSource>,
    kubo: wiremock::MockServer,
    server: S3ServerHandle,
}

impl S3TestEndpoint for IntakeHarness {
    fn endpoint(&self) -> &str {
        &self.endpoint
    }
    fn bucket(&self) -> &str {
        &self.bucket
    }
}

impl IntakeHarness {
    async fn kubo_total_call_count(&self) -> usize {
        self.kubo.received_requests().await.unwrap().len()
    }

    async fn shutdown(self) {
        assert_eq!(self.source.calls.load(Ordering::SeqCst), 0);
        assert_eq!(self.kubo_total_call_count().await, 0);
        self.server.shutdown().await;
    }
}

async fn start_intake_harness() -> IntakeHarness {
    let kubo = wiremock::MockServer::start().await;
    let config: ipfs_s3_gateway::config::Config = toml::from_str(&format!(
        "[kubo]\nrpc_url = {:?}\n[storage]\ndatabase_url = \"sqlite::memory:\"\n",
        kubo.uri()
    ))
    .unwrap();
    let state = ipfs_s3_gateway::state::AppState::new(&config)
        .await
        .unwrap();
    let bucket = "test-bkt".to_owned();
    ipfs_s3_gateway::store::bucket::create(state.store.db(), &bucket, None)
        .await
        .unwrap();
    let source = Arc::new(IntakeSource::default());
    let validated = ImportConfig {
        allowed_https_origins: vec!["https://intake.example.test".to_owned()],
        ..ImportConfig::default()
    }
    .validate()
    .unwrap();
    let downloader = SourceDownloader::with_components(
        Arc::new(validated.clone()),
        source.clone(),
        Arc::new(StrictPublicAddressPolicy),
        source.clone(),
    );
    let coordinator = ImportCoordinator::new(validated, downloader);
    let server = start_s3_server_with_imports(
        state.clone(),
        Arc::new(tokio::sync::Mutex::new(Vec::new())),
        coordinator,
    )
    .await;
    // Do not call coordinator.start: real worker tests retain their own fixture.
    IntakeHarness {
        endpoint: server.endpoint.clone(),
        bucket,
        state,
        source,
        kubo,
        server,
    }
}

fn headers(token: &str, expected: Option<&str>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("content-type", "application/xml"),
        ("x-ipfs3-client-token", token),
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "false"),
        ("x-ipfs3-zip-publish-extracted", "true"),
        ("x-ipfs3-zip-targets", "none"),
        ("x-ipfs3-zip-token", token),
    ] {
        headers.insert(name, HeaderValue::from_str(value).unwrap());
    }
    if let Some(expected) = expected {
        headers.insert(
            "x-ipfs3-zip-expected-sha256",
            HeaderValue::from_str(expected).unwrap(),
        );
    }
    headers
}

async fn post(h: &IntakeHarness, key: &str, source: &str, headers: HeaderMap) -> reqwest::Response {
    support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &h.endpoint,
        &h.bucket,
        key,
        &[("ipfs3-import", ""), ("decompress-zip", "out/")],
        source.as_bytes().to_vec(),
        headers,
        "test",
    )
    .await
}

#[tokio::test]
async fn cid_intake_has_no_source_side_effect_and_status_is_read_only() {
    let h = start_intake_harness().await;
    let xml = format!("<IPFS3ImportRequest><CID>{CID}</CID></IPFS3ImportRequest>");
    let first = post(&h, "archive.zip", &xml, headers("intake-cid", None)).await;
    assert_eq!(
        first.status(),
        StatusCode::ACCEPTED,
        "{}",
        first.text().await.unwrap()
    );
    let id = first.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let stored = execution::read(h.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    let captured: serde_json::Value = serde_json::from_str(&stored.captured_options).unwrap();
    assert_eq!(captured["options"]["token"], "intake-cid");
    assert_eq!(
        captured["rule_revision"].as_str(),
        Some(h.state.pinning.zip_output_rules().revision())
    );
    let same = post(&h, "archive.zip", &xml, headers("intake-cid", None)).await;
    assert_eq!(same.status(), StatusCode::ACCEPTED);
    assert_eq!(same.headers()["x-ipfs3-import-job-id"], id);
    let status = support::sigv4::send_sigv4(
        reqwest::Method::GET,
        &h.endpoint,
        &h.bucket,
        "archive.zip",
        &[("ipfs3-import", &id)],
        vec![],
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(status.status(), StatusCode::OK);
    let text = status.text().await.unwrap();
    assert!(text.contains("<State>pending</State>"), "{text}");
    assert!(
        text.contains("<MeasuredSHA256>unknown</MeasuredSHA256>"),
        "{text}"
    );
    assert!(!text.contains("<Artifact>"));
    let claim = execution::claim(h.state.store.db(), &id, "failed-worker", 20)
        .await
        .unwrap()
        .unwrap();
    assert!(execution::fence(h.state.store.db(), &claim).await.unwrap());
    let failed = support::sigv4::send_sigv4(
        reqwest::Method::GET,
        &h.endpoint,
        &h.bucket,
        "archive.zip",
        &[("ipfs3-import", &id)],
        vec![],
        HeaderMap::new(),
        "test",
    )
    .await;
    let failed_xml = failed.text().await.unwrap();
    assert!(failed_xml.contains("<State>failed</State>"), "{failed_xml}");
    assert!(failed_xml.contains("<MeasuredSHA256>unknown</MeasuredSHA256>"));
    let failed_replay = post(&h, "archive.zip", &xml, headers("intake-cid", None)).await;
    assert_eq!(failed_replay.headers()["x-ipfs3-import-job-id"], id);
    let changed = post(
        &h,
        "archive.zip",
        &xml,
        headers("intake-cid", Some(&"a".repeat(64))),
    )
    .await;
    assert_eq!(changed.status(), StatusCode::CONFLICT);
    assert_eq!(
        import_job::Entity::find()
            .count(h.state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        object::Entity::find()
            .count(h.state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        object_version::Entity::find()
            .count(h.state.store.db())
            .await
            .unwrap(),
        0
    );
    for count in [
        import_destination::Entity::find()
            .count(h.state.store.db())
            .await
            .unwrap(),
        import_prefix_claim::Entity::find()
            .count(h.state.store.db())
            .await
            .unwrap(),
        standard_mutation_lease::Entity::find()
            .count(h.state.store.db())
            .await
            .unwrap(),
    ] {
        assert_eq!(count, 0, "v2 admission must not claim archive or outputs");
    }
    assert_eq!(h.kubo_total_call_count().await, 0);
    h.shutdown().await;
}

#[tokio::test]
async fn url_fixed_digest_replay_never_fetches_changed_source() {
    let h = start_intake_harness().await;
    let url = h.source.url("/secret.zip?credential=never-render");
    let xml = format!("<IPFS3ImportRequest><URL>{url}</URL></IPFS3ImportRequest>");
    let expected = "b".repeat(64);
    let first = post(
        &h,
        "archive.zip",
        &xml,
        headers("intake-url", Some(&expected)),
    )
    .await;
    assert_eq!(
        first.status(),
        StatusCode::ACCEPTED,
        "{}",
        first.text().await.unwrap()
    );
    let id = first.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let replay = post(
        &h,
        "archive.zip",
        &xml,
        headers("intake-url", Some(&expected)),
    )
    .await;
    assert_eq!(replay.status(), StatusCode::ACCEPTED);
    assert_eq!(replay.headers()["x-ipfs3-import-job-id"], id);
    let different = post(
        &h,
        "archive.zip",
        &xml,
        headers("intake-url", Some(&"c".repeat(64))),
    )
    .await;
    assert_eq!(different.status(), StatusCode::CONFLICT);
    let other_url = h.source.url("/new-secret.zip?credential=changed");
    let other_xml = format!("<IPFS3ImportRequest><URL>{other_url}</URL></IPFS3ImportRequest>");
    let changed_url = post(
        &h,
        "archive.zip",
        &other_xml,
        headers("intake-url", Some(&expected)),
    )
    .await;
    assert_eq!(changed_url.status(), StatusCode::CONFLICT);
    let mut changed_metadata = headers("intake-url", Some(&expected));
    changed_metadata.insert("x-amz-meta-note", HeaderValue::from_static("new"));
    assert_eq!(
        post(&h, "archive.zip", &xml, changed_metadata)
            .await
            .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        post(
            &h,
            "other.zip",
            &xml,
            headers("intake-url", Some(&expected))
        )
        .await
        .status(),
        StatusCode::CONFLICT
    );
    let mut changed_tags = headers("intake-url", Some(&expected));
    changed_tags.insert("x-amz-tagging", HeaderValue::from_static("purpose=changed"));
    assert_eq!(
        post(&h, "archive.zip", &xml, changed_tags).await.status(),
        StatusCode::CONFLICT
    );
    ipfs_s3_gateway::store::bucket::create(h.state.store.db(), "other-bkt", None)
        .await
        .unwrap();
    let other_bucket = support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &h.endpoint,
        "other-bkt",
        "archive.zip",
        &[("ipfs3-import", ""), ("decompress-zip", "out/")],
        xml.as_bytes().to_vec(),
        headers("intake-url", Some(&expected)),
        "test",
    )
    .await;
    assert_eq!(other_bucket.status(), StatusCode::CONFLICT);
    let missing = post(&h, "other.zip", &xml, headers("missing-digest", None)).await;
    assert_eq!(missing.status(), StatusCode::BAD_REQUEST);
    let uppercase = post(
        &h,
        "other.zip",
        &xml,
        headers("uppercase", Some(&"A".repeat(64))),
    )
    .await;
    assert_eq!(uppercase.status(), StatusCode::BAD_REQUEST);
    let status = support::sigv4::send_sigv4(
        reqwest::Method::GET,
        &h.endpoint,
        &h.bucket,
        "archive.zip",
        &[("ipfs3-import", &id)],
        vec![],
        HeaderMap::new(),
        "test",
    )
    .await;
    let text = status.text().await.unwrap();
    assert!(text.contains(&format!("<ExpectedSHA256>{expected}</ExpectedSHA256>")));
    assert!(!text.contains("never-render"));
    let ids = import_intake::pending_ids(h.state.store.db(), "", 10)
        .await
        .unwrap();
    assert_eq!(ids, vec![id.clone()]);
    let claim = execution::claim(h.state.store.db(), &id, "future-worker", 20)
        .await
        .unwrap()
        .unwrap();
    let private = import_intake::claimed_source(h.state.store.db(), &claim)
        .await
        .unwrap()
        .unwrap();
    assert!(private.source_descriptor.contains("never-render"));
    assert_eq!(private.expected_sha256.as_deref(), Some(expected.as_str()));
    assert!(
        import_intake::bind_verified_input(h.state.store.db(), &claim, &"c".repeat(64), CID, 0)
            .await
            .is_err()
    );
    assert!(
        execution::read(h.state.store.db(), &id)
            .await
            .unwrap()
            .unwrap()
            .input_sha256
            .is_none()
    );
    assert_eq!(h.source.calls.load(Ordering::SeqCst), 0);
    assert_eq!(h.kubo_total_call_count().await, 0);
    h.shutdown().await;
}

#[tokio::test]
async fn cross_protocol_token_conflicts_both_directions_and_rejections_never_claim_archive() {
    let h = start_intake_harness().await;
    let xml = format!("<IPFS3ImportRequest><CID>{CID}</CID></IPFS3ImportRequest>");
    assert_eq!(
        post(&h, "v2.zip", &xml, headers("shared-v2", None))
            .await
            .status(),
        StatusCode::ACCEPTED
    );
    let legacy = post_import(
        &h,
        &h.bucket,
        "v2.zip",
        "ipfs3-import&decompress-zip=out%2F",
        &xml,
        Some("shared-v2"),
    )
    .await;
    assert_eq!(legacy.status(), StatusCode::CONFLICT);
    let first = post_import(
        &h,
        &h.bucket,
        "legacy.zip",
        "ipfs3-import&decompress-zip=out%2F",
        &xml,
        Some("shared-old"),
    )
    .await;
    assert_eq!(first.status(), StatusCode::ACCEPTED);
    assert_eq!(
        post(&h, "legacy.zip", &xml, headers("shared-old", None))
            .await
            .status(),
        StatusCode::CONFLICT
    );
    let mut bad_token = headers("signed-zip-token", None);
    bad_token.insert(
        "x-ipfs3-client-token",
        HeaderValue::from_static("different"),
    );
    assert_eq!(
        post(&h, "unclaimed.zip", &xml, bad_token).await.status(),
        StatusCode::BAD_REQUEST
    );
    let mut missing = headers("no-token", None);
    missing.remove("x-ipfs3-client-token");
    assert_eq!(
        post(&h, "unclaimed.zip", &xml, missing).await.status(),
        StatusCode::BAD_REQUEST
    );
    let mut source_mode = headers("source-mode", None);
    source_mode.insert(
        "x-ipfs3-zip-publish-source",
        HeaderValue::from_static("true"),
    );
    let source_accepted = post(&h, "unclaimed.zip", &xml, source_mode).await;
    assert_eq!(source_accepted.status(), StatusCode::ACCEPTED);
    assert!(!source_accepted.headers().contains_key("etag"));
    assert!(!source_accepted.headers().contains_key("x-amz-version-id"));
    let source_id = source_accepted.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap();
    let source_snapshot = execution::read(h.state.store.db(), source_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source_snapshot.state, "pending");
    assert!(source_snapshot.input_art_cid.is_none());
    let capture: serde_json::Value =
        serde_json::from_str(&source_snapshot.captured_options).unwrap();
    assert_eq!(capture["options"]["publish_source"], true);
    assert_eq!(capture["options"]["publish_extracted"], true);
    assert_eq!(
        object_version::Entity::find()
            .count(h.state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        standard_mutation_lease::Entity::find()
            .count(h.state.store.db())
            .await
            .unwrap(),
        0
    );
    assert!(
        ipfs_s3_gateway::store::zip::snapshot(h.state.store.db(), source_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        import_job::Entity::find()
            .count(h.state.store.db())
            .await
            .unwrap(),
        1
    );
    let accepted = post(&h, "unclaimed.zip", &xml, headers("valid-only", None)).await;
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);
    let id = accepted.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap();
    let wrong_key = support::sigv4::send_sigv4(
        reqwest::Method::GET,
        &h.endpoint,
        &h.bucket,
        "elsewhere.zip",
        &[("ipfs3-import", id)],
        vec![],
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(wrong_key.status(), StatusCode::NOT_FOUND);
    h.shutdown().await;
}

#[tokio::test]
async fn request_insert_failure_rolls_back_execution_row() {
    let h = start_intake_harness().await;
    h.state.store.db().execute_unprepared("CREATE TRIGGER block_zip_intake BEFORE INSERT ON zip_v2_import_requests BEGIN SELECT RAISE(ABORT, 'injected_failure'); END").await.unwrap();
    let xml = format!("<IPFS3ImportRequest><CID>{CID}</CID></IPFS3ImportRequest>");
    let response = post(&h, "archive.zip", &xml, headers("atomic", None)).await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let rows = h
        .state
        .store
        .db()
        .query_one(sea_orm::Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS n FROM zip_v2_executions",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rows.try_get::<i64>("", "n").unwrap(), 0);
    h.shutdown().await;
}

#[tokio::test]
async fn unsigned_http_v2_never_reaches_intake() {
    let h = start_intake_harness().await;
    let url = format!(
        "{}/{}/archive.zip?ipfs3-import&decompress-zip=out%2F",
        h.endpoint, h.bucket
    );
    let xml = format!("<IPFS3ImportRequest><CID>{CID}</CID></IPFS3ImportRequest>");
    let response = reqwest::Client::new()
        .post(url)
        .headers(headers("unsigned", None))
        .body(xml)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(
        import_intake::pending_ids(h.state.store.db(), "", 10)
            .await
            .unwrap()
            .is_empty()
    );
    h.shutdown().await;
}

#[tokio::test]
async fn concurrent_same_token_admits_one_atomic_batch() {
    let h = start_intake_harness().await;
    let xml = format!("<IPFS3ImportRequest><CID>{CID}</CID></IPFS3ImportRequest>");
    let (left, right) = tokio::join!(
        post(&h, "archive.zip", &xml, headers("concurrent", None)),
        post(&h, "archive.zip", &xml, headers("concurrent", None))
    );
    assert_eq!(left.status(), StatusCode::ACCEPTED);
    assert_eq!(right.status(), StatusCode::ACCEPTED);
    assert_eq!(
        left.headers()["x-ipfs3-import-job-id"],
        right.headers()["x-ipfs3-import-job-id"]
    );
    assert_eq!(
        import_intake::pending_ids(h.state.store.db(), "", 10)
            .await
            .unwrap()
            .len(),
        1
    );
    h.shutdown().await;
}

/// Explicit PostgreSQL mode: uses a UUID-named disposable schema, never public.
/// Run with IPFS_S3_TEST_POSTGRES_URL set and `--ignored`.
#[tokio::test]
#[ignore = "requires explicitly configured isolated PostgreSQL test database"]
async fn postgres_intake_is_atomic_and_cross_protocol_conflict_is_bidirectional() {
    use futures_util::FutureExt as _;
    use ipfs_s3_gateway::store::{self, zip::execution::Admission};
    use sea_orm::{ConnectOptions, Database, DatabaseBackend, Statement};
    use sha2::{Digest, Sha256};
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL").expect("configure test PostgreSQL URL");
    let admin = Database::connect(&url).await.unwrap();
    let schema = format!("zip_import_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let result = std::panic::AssertUnwindSafe(async {
        let mut options = ConnectOptions::new(&url);
        options.min_connections(1).max_connections(1);
        let db = Database::connect(options).await.unwrap();
        db.execute_unprepared(&format!("SET search_path TO {schema}"))
            .await
            .unwrap();
        store::run_migrations(&db).await.unwrap();
        store::bucket::create(&db, "bucket", None).await.unwrap();
        let contract = "import-zip-v2-pg-test".to_owned();
        let request = import_intake::Request {
            admission: Admission {
                id: uuid::Uuid::new_v4().to_string(),
                owner: "test".into(),
                source: "import".into(),
                token: "pg-token".into(),
                request_fingerprint: hex::encode(Sha256::digest(contract.as_bytes())),
                request_contract: contract,
                bucket: "bucket".into(),
                source_key: "archive.zip".into(),
                captured_options: "{}".into(),
            },
            prefix: "out/".into(),
            source_descriptor: format!("[\"cid\",\"{CID}\"]"),
            expected_sha256: None,
        };
        let first = import_intake::admit(&db, &request).await.unwrap();
        assert_eq!(first.state, "pending");
        assert_eq!(
            import_intake::admit(&db, &request).await.unwrap().id,
            first.id
        );
        assert!(
            import_intake::legacy_token_exists(&db, "bucket", "archive.zip", "pg-token")
                .await
                .unwrap()
        );
        let denied = db
            .execute(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "INSERT INTO import_jobs (id,bucket,key,client_token) VALUES ($1,$2,$3,$4)",
                vec![
                    "invalid-job".into(),
                    "bucket".into(),
                    "archive.zip".into(),
                    "pg-token".into(),
                ],
            ))
            .await;
        assert!(
            denied
                .unwrap_err()
                .to_string()
                .contains("zip_v2_import_token_conflict")
        );
        assert!(execution::read(&db, &first.id).await.unwrap().is_some());
        db.close().await.unwrap();
    })
    .catch_unwind()
    .await;
    admin
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    admin.close().await.unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
