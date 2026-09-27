//! Signed, real-binary MPU admission replay across independent gateway processes.
use std::{
    fs::File,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use http::{HeaderMap, HeaderValue};
use ipfs_s3_gateway::{
    pinning::decision::{DecisionEffect, ExtensionDecision},
    store::{
        self,
        entities::{multipart_upload, object, pin_job, pin_lease, pin_lease_target, remote_pin},
    },
};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[path = "support/sigv4.rs"]
#[allow(dead_code)]
mod sigv4;

const BUCKET: &str = "stage3-restart";
const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const PART: &[u8] = b"payload";

// The test owns the child process and its log. Never leave a gateway writing to
// the SQLite file after the test-owned temporary directory is removed.
struct Gateway {
    child: Child,
    log: PathBuf,
    endpoint: String,
}

impl Gateway {
    fn stop(&mut self) {
        if self
            .child
            .try_wait()
            .expect("inspect gateway status")
            .is_none()
        {
            self.child
                .kill()
                .unwrap_or_else(|error| panic!("kill test-owned gateway {:?}: {error}", self.log));
        }
        self.child.wait().expect("reap test-owned gateway");
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.stop();
    }
}

fn available_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback test port");
    listener.local_addr().unwrap().port()
}

fn config(
    db_url: &str,
    kubo: &MockServer,
    pinata: &MockServer,
    port: u16,
    credential: u64,
    endpoint: u64,
) -> String {
    format!(
        r#"
[server]
bind = "127.0.0.1:{port}"
[kubo]
rpc_url = "{kubo_url}"
[storage]
database_url = "{db_url}"
[auth]
credentials = [{{ access_key = "test", secret_key = "test" }}]
[pinning_identity]
primary_storage_domain = "local"
[[pinning_identity.providers]]
config_name = "remote"
provider_id = "stable-remote"
display_name = "Test Pinata"
backend = "pinata"
scope = "test-account"
storage_domain = "remote"
credential_revision = {credential}
endpoint_revision = {endpoint}
secret_ref = "env:STAGE3_RESTART_PINATA_TOKEN"
api_profile = "pinata-v3"
strategy = "cid"
[[pinning.providers]]
name = "remote"
kind = "pinata"
api = "v3"
strategy = "cid"
token_env = "STAGE3_RESTART_PINATA_TOKEN"
endpoint = "{pinata_url}/v3"
priority = 1
max_bytes = 100000
max_pins = 100
[[pinning.policies]]
bucket = "{BUCKET}"
trigger = "request"
provider_mode = "one"
providers = ["remote"]
default_duration = "1h"
max_duration = "24h"
[pinning]
worker_interval = "1h"
"#,
        kubo_url = kubo.uri(),
        pinata_url = pinata.uri(),
    )
}

async fn start_gateway(directory: &Path, config_file: &Path, port: u16, label: &str) -> Gateway {
    let log = directory.join(format!("{label}.log"));
    let output = File::create(&log).expect("create test-owned gateway log");
    let mut child = Command::new(env!("CARGO_BIN_EXE_ipfs-s3-gateway"))
        .env("IPFS_S3_CONFIG", config_file)
        .env("STAGE3_RESTART_PINATA_TOKEN", "test-only-token")
        .env_remove("IPFS_S3_BIND")
        .env_remove("IPFS_S3_DATABASE_URL")
        .env_remove("IPFS_S3_KUBO_RPC_URL")
        .env_remove("IPFS_S3_ACCESS_KEY_ID")
        .env_remove("IPFS_S3_SECRET_ACCESS_KEY")
        .env_remove("IPFS_S3_MASTER_KEY")
        .env_remove("IPFS_S3_COLD_KUBO_RPC_URL")
        .stdout(Stdio::from(output.try_clone().unwrap()))
        .stderr(Stdio::from(output))
        .spawn()
        .expect("spawn real gateway binary");
    let endpoint = format!("http://127.0.0.1:{port}");
    let deadline = Instant::now() + Duration::from_secs(15);
    let client = reqwest::Client::new();
    loop {
        if let Some(status) = child.try_wait().expect("inspect gateway startup") {
            child.wait().expect("reap failed gateway startup");
            panic!(
                "gateway {label} exited {status}: {}",
                std::fs::read_to_string(&log).unwrap_or_default()
            );
        }
        if let Ok(response) = client.get(format!("{endpoint}/ready")).send().await
            && response.status().is_success()
            && response.text().await.unwrap_or_default() == "READY"
        {
            break;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "gateway {label} readiness timed out: {}",
                std::fs::read_to_string(&log).unwrap_or_default()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Gateway {
        child,
        log,
        endpoint,
    }
}

fn upload_id(xml: &str) -> &str {
    xml.split("<UploadId>")
        .nth(1)
        .expect("create MPU returned UploadId")
        .split("</UploadId>")
        .next()
        .unwrap()
}

async fn admit(endpoint: &str, key: &str) -> String {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-tagging",
        HeaderValue::from_static("ipfs-s3%3Apin=true"),
    );
    let created = sigv4::send_sigv4(
        reqwest::Method::POST,
        endpoint,
        BUCKET,
        key,
        &[("uploads", "")],
        Vec::new(),
        headers,
        "test",
    )
    .await;
    let status = created.status();
    let body = created.text().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::OK, "create {key}: {body}");
    let id = upload_id(&body).to_owned();
    let part = sigv4::send_sigv4(
        reqwest::Method::PUT,
        endpoint,
        BUCKET,
        key,
        &[("partNumber", "1"), ("uploadId", &id)],
        PART.to_vec(),
        HeaderMap::new(),
        "test",
    )
    .await;
    let status = part.status();
    let body = part.text().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::OK, "upload part {key}: {body}");
    id
}

async fn complete(endpoint: &str, key: &str, id: &str) -> (reqwest::StatusCode, String) {
    let body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{CID}\"</ETag></Part></CompleteMultipartUpload>"
    );
    let response = sigv4::send_sigv4(
        reqwest::Method::POST,
        endpoint,
        BUCKET,
        key,
        &[("uploadId", id)],
        body.into_bytes(),
        HeaderMap::new(),
        "test",
    )
    .await;
    (response.status(), response.text().await.unwrap())
}

async fn decision(db: &DatabaseConnection, id: &str) -> ExtensionDecision {
    let row = multipart_upload::Entity::find_by_id(id)
        .one(db)
        .await
        .unwrap()
        .expect("unconsumed upload must persist");
    store::multipart::decision_from_upload(&row)
        .unwrap()
        .unwrap()
}

#[derive(Debug, PartialEq, Eq)]
struct Rows {
    objects: u64,
    uploads: u64,
    leases: u64,
    targets: u64,
    remotes: u64,
    jobs: u64,
}

async fn rows(db: &DatabaseConnection) -> Rows {
    Rows {
        objects: object::Entity::find().count(db).await.unwrap(),
        uploads: multipart_upload::Entity::find().count(db).await.unwrap(),
        leases: pin_lease::Entity::find().count(db).await.unwrap(),
        targets: pin_lease_target::Entity::find().count(db).await.unwrap(),
        remotes: remote_pin::Entity::find().count(db).await.unwrap(),
        jobs: pin_job::Entity::find().count(db).await.unwrap(),
    }
}

async fn kubo_counts(kubo: &MockServer) -> [usize; 3] {
    let requests = kubo.received_requests().await.unwrap();
    ["/api/v0/cat", "/api/v0/add", "/api/v0/pin/add"].map(|path| {
        requests
            .iter()
            .filter(|request| request.url.path() == path)
            .count()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepted_mpu_survives_real_restart_and_revision_changes_fail_before_io() {
    let directory = tempfile::tempdir().expect("own test files");
    let kubo = MockServer::start().await;
    let pinata = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Hash\":\"{CID}\",\"Size\":\"7\"}}\n")),
        )
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(PART))
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{CID}\"]}}")),
        )
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/v3/files/public/pin_by_cid"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"data": {"id": "test-request", "status": "pinned", "cid": CID}}),
        ))
        .mount(&pinata)
        .await;
    Mock::given(method("GET"))
        .and(path("/v3/files/public"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"data": {"files": [{"id": "test-request", "cid": CID}]}}),
        ))
        .mount(&pinata)
        .await;

    let db_path = directory.path().join("gateway.sqlite");
    let db_url = format!(
        "sqlite://{}?mode=rwc",
        db_path.display().to_string().replace('\\', "/")
    );
    let config_file = directory.path().join("config.toml");
    let port = available_port();
    std::fs::write(&config_file, config(&db_url, &kubo, &pinata, port, 1, 1)).unwrap();
    let mut a = start_gateway(directory.path(), &config_file, port, "process-a").await;
    let bucket = sigv4::send_sigv4(
        reqwest::Method::PUT,
        &a.endpoint,
        BUCKET,
        "",
        &[],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    let status = bucket.status();
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "bucket: {}",
        bucket.text().await.unwrap()
    );
    let good = admit(&a.endpoint, "good").await;
    let credential = admit(&a.endpoint, "credential").await;
    let endpoint_id = admit(&a.endpoint, "endpoint").await;
    let db = store::connect_database(&db_url).await.unwrap();
    let captured = decision(&db, &good).await;
    assert_eq!(captured.effect, DecisionEffect::Accepted);
    assert_eq!(captured.origin.principal_id, "test");
    assert_eq!(captured.origin.request_id, good);
    assert!(captured.config_revision.starts_with("d91d726f120fab02"));
    assert_eq!(
        decision(&db, &credential).await.config_revision,
        captured.config_revision
    );
    assert_eq!(
        decision(&db, &endpoint_id).await.config_revision,
        captured.config_revision
    );
    assert_eq!(
        rows(&db).await,
        Rows {
            objects: 0,
            uploads: 3,
            leases: 0,
            targets: 0,
            remotes: 0,
            jobs: 0
        }
    );
    assert_eq!(kubo_counts(&kubo).await, [0, 3, 3]);
    a.stop(); // A is fully reaped before B opens the same SQLite file.

    let mut b = start_gateway(directory.path(), &config_file, port, "process-b-stable").await;
    assert_eq!(decision(&db, &good).await, captured);
    let (status, body) = complete(&b.endpoint, "good", &good).await;
    assert_eq!(status, reqwest::StatusCode::OK, "stable replay: {body}");
    assert!(body.contains(CID));
    assert_eq!(
        object::Entity::find()
            .filter(object::Column::Key.eq("good"))
            .count(&db)
            .await
            .unwrap(),
        1
    );
    assert!(
        multipart_upload::Entity::find_by_id(&good)
            .one(&db)
            .await
            .unwrap()
            .is_none()
    );
    let published = rows(&db).await;
    assert_eq!(published.objects, 1);
    assert_eq!(published.uploads, 2);
    assert_eq!(
        published.leases, 1,
        "captured pin intent must publish, not just the S3 object"
    );
    assert_eq!(published.targets, 1);
    assert_eq!(published.remotes, 1);
    assert_eq!(published.jobs, 1);
    assert_eq!(kubo_counts(&kubo).await, [1, 4, 4]);
    let root = object::Entity::find()
        .filter(object::Column::Key.eq("good"))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(root.cid, CID);
    assert_eq!(root.size, PART.len() as i64);
    let adds = kubo.received_requests().await.unwrap();
    let adds: Vec<_> = adds
        .iter()
        .filter(|request| request.url.path() == "/api/v0/add")
        .collect();
    assert_eq!(adds.len(), 4);
    assert!(
        adds.iter().all(|request| request
            .body
            .windows(PART.len())
            .any(|window| window == PART)),
        "part and reassembled root add must contain the entire payload"
    );
    b.stop();

    for (label, changed_credential, changed_endpoint, key, id) in [
        ("credential", 2, 1, "credential", &credential),
        ("endpoint", 1, 2, "endpoint", &endpoint_id),
    ] {
        std::fs::write(
            &config_file,
            config(
                &db_url,
                &kubo,
                &pinata,
                port,
                changed_credential,
                changed_endpoint,
            ),
        )
        .unwrap();
        let mut changed = start_gateway(directory.path(), &config_file, port, label).await;
        let before_rows = rows(&db).await;
        let before_kubo = kubo_counts(&kubo).await;
        let before_pinata = pinata.received_requests().await.unwrap().len();
        let before_decision = decision(&db, id).await;
        let (status, body) = complete(&changed.endpoint, key, id).await;
        assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{label}: {body}");
        assert!(
            body.contains("captured pinning configuration revision is unavailable"),
            "{label}: {body}"
        );
        assert_eq!(
            decision(&db, id).await,
            before_decision,
            "{label} changed capture"
        );
        assert_eq!(
            rows(&db).await,
            before_rows,
            "{label} published/queued work"
        );
        assert_eq!(
            object::Entity::find()
                .filter(object::Column::Key.eq(key))
                .count(&db)
                .await
                .unwrap(),
            0,
            "{label} published an object"
        );
        assert_eq!(
            kubo_counts(&kubo).await,
            before_kubo,
            "{label} invoked cat/add/pin"
        );
        assert_eq!(
            pinata.received_requests().await.unwrap().len(),
            before_pinata,
            "{label} reached Pinata"
        );
        changed.stop();
    }
    assert_eq!(rows(&db).await, published);
    drop(db);
    // All child processes are reaped before the mock servers and temporary directory drop.
}
