//! Native transaction/timer competition in the ZIP-v2 worker; no fake settlement.
use super::*;
use crate::{
    config::Config,
    import::{ImportConfig, downloader::SourceDownloader},
    pinning::{
        decision::DecisionOrigin,
        zip_policy::{ZipCaptureContext, capture_zip_intent},
    },
    store::{self, entities::object_version},
    zip::options::{ZipOptions, parse_authenticated_zip_controls},
};
use sea_orm::{EntityTrait, PaginatorTrait};
use sha2::{Digest, Sha256};
use std::sync::{LazyLock, Mutex};
use tokio::sync::Notify;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const BODY: &[u8] = b"complete source-only input";
const BUDGET: Duration = Duration::from_secs(10);

#[derive(Default)]
struct Gate {
    transaction_held: Notify,
    release_transaction: Notify,
    renewal_pending: Notify,
    snapshot: Mutex<Option<execution::Snapshot>>,
}

static GATES: LazyLock<Mutex<BTreeMap<String, Arc<Gate>>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

fn gate(id: &str) -> Option<Arc<Gate>> {
    GATES.lock().unwrap().get(id).cloned()
}

pub(super) fn renewal_pending(id: &str) {
    if let Some(gate) = gate(id) {
        gate.renewal_pending.notify_one();
    }
}

pub(super) async fn pause_before_admission_commit(id: &str, tx: &sea_orm::DatabaseTransaction) {
    if let Some(gate) = gate(id) {
        *gate.snapshot.lock().unwrap() = execution::read(tx, id).await.unwrap();
        gate.transaction_held.notify_one();
        gate.release_transaction.notified().await;
    }
}

struct Fixture {
    state: Arc<AppState>,
    claim: execution::Claim,
    coordinator: Arc<ImportCoordinator>,
    kubo: MockServer,
    gate: Arc<Gate>,
}

#[derive(Default)]
struct PublicationGate {
    entered: Notify,
    release: Notify,
}

#[async_trait::async_trait]
impl crate::import::pipeline::ImportExecutionObserver for PublicationGate {
    async fn before_publication(&self, _: &str) {
        self.entered.notify_one();
        self.release.notified().await;
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        GATES.lock().unwrap().remove(&self.claim.batch_id);
    }
}

async fn fixture() -> Fixture {
    let kubo = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(BODY))
        .expect(2)
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{CID}\"]}}")),
        )
        .expect(1)
        .mount(&kubo)
        .await;
    let mut config = Config::default_for_test();
    config.kubo.rpc_url = kubo.uri();
    config.storage.database_url = "sqlite::memory:".into();
    config.decompress_zip.auto_pin_outputs = Some(false);
    config.decompress_zip.unixfs_directory_root = false;
    let state = AppState::new(&config).await.unwrap();
    store::bucket::create(state.store.db(), "bucket", None)
        .await
        .unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let mut headers = http::HeaderMap::new();
    for (name, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "true"),
        ("x-ipfs3-zip-publish-extracted", "false"),
        ("x-ipfs3-zip-targets", "none"),
        ("x-ipfs3-zip-token", id.as_str()),
    ] {
        headers.insert(name, http::HeaderValue::from_str(value).unwrap());
    }
    let controls = parse_authenticated_zip_controls(
        &headers,
        &[("decompress-zip".into(), "out/".into())],
        &[],
        true,
        false,
    )
    .unwrap()
    .unwrap();
    let ZipOptions::V2(options) = &controls.options else {
        panic!("v2 fixture");
    };
    let capture = capture_zip_intent(
        state.pinning.effective_config(),
        state.pinning.decompress_zip_config(),
        state.pinning.control_mode(),
        ZipCaptureContext {
            bucket: "bucket",
            source_key: "archive.zip",
            target_prefix: "out/",
            tags: &[],
        },
        &controls,
        DecisionOrigin::new("test", &id),
    )
    .unwrap();
    let contract = serde_json::json!(["native-liveness-source-only", id, CID]).to_string();
    import_intake::admit(
        state.store.db(),
        &import_intake::Request {
            admission: execution::Admission {
                id: id.clone(),
                owner: "test".into(),
                source: "import".into(),
                token: id.clone(),
                request_fingerprint: hex::encode(Sha256::digest(contract.as_bytes())),
                request_contract: contract,
                bucket: "bucket".into(),
                source_key: "archive.zip".into(),
                captured_options: serde_json::json!({"zip_intent":capture, "options":options,
                    "source_tags":[], "source_content_type":null, "source_metadata":null})
                .to_string(),
            },
            prefix: "out/".into(),
            source_descriptor: serde_json::to_string(&("cid", CID)).unwrap(),
            expected_sha256: Some(hex::encode(Sha256::digest(BODY))),
        },
    )
    .await
    .unwrap();
    let claim = execution::claim(state.store.db(), &id, "liveness-worker", 60)
        .await
        .unwrap()
        .unwrap();
    let validated = ImportConfig {
        lease_duration_secs: 2,
        job_timeout_secs: 30,
        ..Default::default()
    }
    .validate()
    .unwrap();
    let coordinator = ImportCoordinator::new(
        validated.clone(),
        SourceDownloader::production(Arc::new(validated)),
    );
    let gate = Arc::new(Gate::default());
    GATES.lock().unwrap().insert(id, gate.clone());
    Fixture {
        state,
        claim,
        coordinator,
        kubo,
        gate,
    }
}

async fn compete_with_heartbeat(
    fixture: &Fixture,
    shutdown: &CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let task = tokio::spawn(execute_with_heartbeat(
        fixture.coordinator.clone(),
        fixture.state.clone(),
        fixture.claim.clone(),
        shutdown.clone(),
        2,
    ));
    tokio::time::timeout(BUDGET, fixture.gate.transaction_held.notified())
        .await
        .expect("real work holds admission transaction");
    let snapshot = fixture.gate.snapshot.lock().unwrap().clone().unwrap();
    assert_eq!(snapshot.state, "admitted");
    assert_eq!(
        snapshot.input_sha256,
        Some(hex::encode(Sha256::digest(BODY)))
    );
    assert_eq!(snapshot.input_art_size, Some(BODY.len() as i64));
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(667)).await;
    // This event is emitted only after the real native renewal future polls
    // Pending while work holds the transaction, not just on a timer tick.
    tokio::time::timeout(BUDGET, fixture.gate.renewal_pending.notified())
        .await
        .expect("native heartbeat is waiting on held work transaction");
    tokio::time::resume();
    assert!(!task.is_finished(), "worker still owns both live futures");
    task
}

#[tokio::test]
async fn held_work_transaction_keeps_progressing_while_native_heartbeat_waits() {
    let fixture = fixture().await;
    let shutdown = CancellationToken::new();
    let mut task = compete_with_heartbeat(&fixture, &shutdown).await;
    fixture.gate.release_transaction.notify_one();
    let result = tokio::time::timeout(BUDGET, &mut task).await;
    if result.is_err() {
        shutdown.cancel();
        task.abort();
        let _ = task.await;
    }
    assert!(
        matches!(result, Ok(Ok(()))),
        "work stopped being polled while its native heartbeat awaited the transaction it held"
    );
    let db = fixture.state.store.db();
    assert_eq!(
        import_intake::read_for_path(db, &fixture.claim.batch_id, "test", "bucket", "archive.zip")
            .await
            .unwrap()
            .unwrap()
            .state,
        "ready"
    );
    assert_eq!(object_version::Entity::find().count(db).await.unwrap(), 1);
    fixture.kubo.verify().await;
}

#[tokio::test]
async fn shutdown_drops_held_work_transaction_even_while_native_heartbeat_waits() {
    let fixture = fixture().await;
    let shutdown = CancellationToken::new();
    let mut task = compete_with_heartbeat(&fixture, &shutdown).await;
    shutdown.cancel();
    let result = tokio::time::timeout(BUDGET, &mut task).await;
    if result.is_err() {
        task.abort();
        let _ = task.await;
    }
    assert!(
        matches!(result, Ok(Ok(()))),
        "shutdown was not observed while native renewal waited on work's transaction"
    );
    let db = fixture.state.store.db();
    let snapshot = execution::read(db, &fixture.claim.batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.state, "pending");
    assert!(snapshot.terminal_result.is_none());
    assert!(
        zip::snapshot(db, &fixture.claim.batch_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(object_version::Entity::find().count(db).await.unwrap(), 0);
    fixture.kubo.verify().await;
}

#[tokio::test]
async fn failed_native_renewal_drops_blocked_work_without_further_business_io() {
    let fixture = fixture().await;
    GATES.lock().unwrap().remove(&fixture.claim.batch_id);
    let publication = Arc::new(PublicationGate::default());
    let validated = fixture.coordinator.config().clone();
    let coordinator = ImportCoordinator::new_with_observer(
        validated.clone(),
        SourceDownloader::production(Arc::new(validated)),
        publication.clone(),
    );
    let shutdown = CancellationToken::new();
    let mut task = tokio::spawn(execute_with_heartbeat(
        coordinator,
        fixture.state.clone(),
        fixture.claim.clone(),
        shutdown.clone(),
        2,
    ));
    tokio::time::timeout(BUDGET, publication.entered.notified())
        .await
        .unwrap();
    let before = fixture.kubo.received_requests().await.unwrap().len();
    let db = fixture.state.store.db();
    use sea_orm::ConnectionTrait;
    db.execute_unprepared("UPDATE standard_mutation_leases SET lease_until=datetime('now','-1 second') WHERE key='archive.zip'")
        .await
        .unwrap();
    let result = tokio::time::timeout(BUDGET, &mut task).await;
    if result.is_err() {
        shutdown.cancel();
        task.abort();
        let _ = task.await;
    }
    assert!(
        matches!(result, Ok(Ok(()))),
        "failed renewal must cancel the blocked work"
    );
    publication.release.notify_one();
    let snapshot = execution::read(db, &fixture.claim.batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.state, "fenced");
    assert_eq!(snapshot.epoch, fixture.claim.epoch);
    assert_eq!(
        snapshot.input_sha256,
        Some(hex::encode(Sha256::digest(BODY)))
    );
    assert!(
        !zip::snapshot(db, &fixture.claim.batch_id)
            .await
            .unwrap()
            .unwrap()
            .batch
            .source_published
    );
    assert_eq!(object_version::Entity::find().count(db).await.unwrap(), 0);
    assert_eq!(
        fixture.kubo.received_requests().await.unwrap().len(),
        before
    );
    fixture.kubo.verify().await;
}
