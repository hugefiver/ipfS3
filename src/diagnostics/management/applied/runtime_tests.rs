use super::*;
use ipfs_s3_gateway::{
    pinning::management::{db_snapshot, dry_run, evidence, local_config, wire},
    store::{
        self,
        entities::{
            pin_invocation_route, pin_job, pin_lease, pin_replica_observation,
            pin_submit_observation, remote_pin,
        },
        pinning::management::actions::job_authority,
    },
};
use sea_orm::{
    ColumnTrait, DatabaseTransaction, DbErr, EntityTrait, ExecResult, QueryFilter, QueryResult,
    Statement,
};
use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};

#[allow(dead_code)]
#[path = "../../../../tests/support/stage7_management.rs"]
mod publication;

struct ResponseGate {
    armed: AtomicBool,
    entered: Notify,
    release: Semaphore,
}

impl ResponseGate {
    fn new() -> Self {
        Self {
            armed: AtomicBool::new(false),
            entered: Notify::new(),
            release: Semaphore::new(0),
        }
    }

    async fn wait(&self) {
        if self.armed.load(Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
    }
}

#[derive(Clone)]
struct CapturedRequest {
    method: axum::http::Method,
    path: String,
    body: Option<Value>,
}

struct HttpBoundary {
    source: ResponseGate,
    submit: ResponseGate,
    requests: Mutex<Vec<CapturedRequest>>,
}

async fn respond(
    axum::extract::State(boundary): axum::extract::State<Arc<HttpBoundary>>,
    request: axum::extract::Request,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let body = if path == "/psa/pins" && method == axum::http::Method::POST {
        let bytes = axum::body::to_bytes(request.into_body(), 4096)
            .await
            .unwrap();
        Some(serde_json::from_slice::<Value>(&bytes).unwrap())
    } else {
        None
    };
    boundary.requests.lock().unwrap().push(CapturedRequest {
        method: method.clone(),
        path: path.clone(),
        body,
    });
    if method != axum::http::Method::POST {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    }
    match path.as_str() {
        "/api/v0/id" => {
            boundary.source.wait().await;
            axum::Json(json!({"ID": publication::NODE})).into_response()
        }
        "/api/v0/cat" => b"0123456789".as_slice().into_response(),
        "/psa/pins" => {
            boundary.submit.wait().await;
            (axum::http::StatusCode::ACCEPTED, axum::Json(json!({
                "requestid": "dispatched-submit", "status": "queued", "created": "2026-01-01T00:00:00Z",
                "pin": {"cid": publication::CID, "name": "controlled"}, "delegates": [], "info": {}
            }))).into_response()
        }
        _ => axum::http::StatusCode::NOT_FOUND.into_response(),
    }
}

struct BoundaryServer {
    uri: String,
    boundary: Arc<HttpBoundary>,
    stop: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl BoundaryServer {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let uri = format!("http://{}", listener.local_addr().unwrap());
        let boundary = Arc::new(HttpBoundary {
            source: ResponseGate::new(),
            submit: ResponseGate::new(),
            requests: Mutex::new(Vec::new()),
        });
        let router = axum::Router::new()
            .fallback(respond)
            .with_state(boundary.clone());
        let stop = CancellationToken::new();
        let shutdown = stop.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .unwrap();
        });
        Self {
            uri,
            boundary,
            stop,
            task: Some(task),
        }
    }

    fn uri(&self) -> String {
        self.uri.clone()
    }

    fn requests(&self) -> Vec<CapturedRequest> {
        self.boundary.requests.lock().unwrap().clone()
    }

    async fn close(&mut self) {
        self.stop.cancel();
        tokio::time::timeout(Duration::from_secs(5), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap();
    }
}

impl Drop for BoundaryServer {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

struct Fixture {
    directory: tempfile::TempDir,
    db: DatabaseConnection,
    raw: Config,
    config: ManagementConfig,
    auth: LocalAuthorization,
    imported: ImportedPlan,
    job: pin_job::Model,
    outside_job: pin_job::Model,
    outside_lease: pin_lease::Model,
    outside_remote: remote_pin::Model,
    server: BoundaryServer,
}

impl Fixture {
    async fn new(private_bytes: usize) -> Self {
        let server = BoundaryServer::start().await;
        let directory = tempfile::Builder::new()
            .prefix("stage7-runtime-cancel-sol-20261013-")
            .tempdir()
            .unwrap();
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            directory
                .path()
                .join("management.sqlite")
                .to_string_lossy()
                .replace('\\', "/")
        );
        let db = store::connect_database(&database_url).await.unwrap();
        let mut raw = publication::config(&server.uri());
        raw.auth.credentials[0].access_key = "owned-runtime-fixture".into();
        raw.auth.credentials[0].secret_key = "owned-runtime-local-only".into();
        raw.auth.credentials.truncate(1);
        raw.storage.database_url = database_url;
        let provider = &mut raw.pinning.providers[0];
        provider.kind = "pinata".into();
        provider.endpoint = Some(format!("{}/psa", server.uri()));
        provider.token_env = Some("OWNED_RUNTIME_FIXTURE_TOKEN".into());
        let identity = &mut raw.pinning_identity.providers[0];
        identity.backend = "pinata".into();
        identity.api_profile = "pinata-psa".into();
        identity.secret_ref = Some("env:OWNED_RUNTIME_FIXTURE_TOKEN".into());
        let config = local_config::compile(&raw, |_| Some("local-fixture-only".into()))
            .unwrap()
            .with_source_node(publication::NODE.into())
            .unwrap();
        publication::setup(&db, &config).await;
        // Ordinary due work is deliberately outside the confirmed receipt.
        let outside_identity = publication::publish_cid(
            &db,
            &config,
            "outside",
            "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGF",
            true,
        )
        .await;
        let outside_job = pin_job::Entity::find().one(&db).await.unwrap().unwrap();
        let outside_lease = pin_lease::Entity::find().one(&db).await.unwrap().unwrap();
        let outside_remote = remote_pin::Entity::find().one(&db).await.unwrap().unwrap();
        let identity = publication::publish(&db, &config, "owned", false).await;
        publication::write_current_decision(&db, &config, &identity).await;
        if private_bytes != 0 {
            // The original inventory includes this private non-source row.
            db.execute(Statement::from_sql_and_values(
                db.get_database_backend(),
                "UPDATE objects SET metadata=$1 WHERE id=$2",
                [
                    json!({"private": "x".repeat(private_bytes)})
                        .to_string()
                        .into(),
                    outside_identity.object_id.clone().into(),
                ],
            ))
            .await
            .unwrap();
        }
        let auth = records::authorize_local(&raw, &db).await.unwrap();
        evidence::prepare(
            &db,
            &config,
            &auth,
            &KuboClient::new(server.uri()),
            &evidence::PrepareRequest {
                record_id: "runtime-proof".into(),
                identity: identity.clone(),
                expected_node: publication::NODE.into(),
                max_stored_bytes: 10,
                ttl: chrono::Duration::hours(1),
                directory: false,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let request = publication::request(identity, &config, false);
        let snapshot = db_snapshot::collect_with_config(&db, &config, &request)
            .await
            .unwrap()
            .planning_snapshot()
            .unwrap();
        let plan = dry_run(&request, &snapshot).unwrap();
        let binding = records::confirm(
            &db,
            &config,
            &auth,
            (&request, &snapshot, &plan),
            vec![],
            chrono::Duration::minutes(5),
        )
        .await
        .unwrap();
        let work = records::SharedDecodeBudget::default();
        let mut document = Vec::new();
        wire::encode_review(
            &mut work.lock().unwrap(),
            &binding.review(),
            Some(&binding.confirmation.candidate_item_ids),
            &mut document,
        )
        .unwrap();
        let imported = wire::decode_review(&document, work).unwrap();
        let applied = actions::apply(&db, &config, &auth, &binding).await.unwrap();
        let job = pin_job::Entity::find_by_id(applied.items[0].jobs[0].job_id.clone())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        Self {
            directory,
            db,
            raw,
            config,
            auth,
            imported,
            job,
            outside_job,
            outside_lease,
            outside_remote,
            server,
        }
    }

    async fn assert_outside_unchanged(&self) {
        assert_eq!(
            pin_job::Entity::find_by_id(self.outside_job.id.clone())
                .one(&self.db)
                .await
                .unwrap()
                .unwrap(),
            self.outside_job
        );
        assert_eq!(
            pin_lease::Entity::find_by_id(self.outside_lease.id.clone())
                .one(&self.db)
                .await
                .unwrap()
                .unwrap(),
            self.outside_lease
        );
        assert_eq!(
            remote_pin::Entity::find_by_id((
                self.outside_remote.provider.clone(),
                self.outside_remote.cid.clone()
            ))
            .one(&self.db)
            .await
            .unwrap()
            .unwrap(),
            self.outside_remote
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn token_drain_after_claim_loss_keeps_dispatched_submit_receipt_without_projection() {
    let mut fixture = Fixture::new(0).await;
    fixture
        .server
        .boundary
        .submit
        .armed
        .store(true, Ordering::SeqCst);
    let cancellation = CancellationToken::new();
    let command = Command::Apply {
        confirmed: "in-process-confirmed-document".into(),
        run: true,
    };
    let mut work = Box::pin(execute(
        &fixture.db,
        &fixture.raw,
        &fixture.config,
        &fixture.auth,
        &command,
        &fixture.imported,
        cancellation.clone(),
    ));
    tokio::select! {
        _ = fixture.server.boundary.submit.entered.notified() => {},
        result = &mut work => panic!("scoped execution ended before dispatch: {result:?}"),
        _ = tokio::time::sleep(Duration::from_secs(10)) => panic!("scoped submit did not dispatch"),
    }
    let original_invocation = pin_invocation_route::Entity::find_by_id(fixture.job.id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let dispatched_claim = pin_job::Entity::find_by_id(fixture.job.id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let old_claim = dispatched_claim
        .locked_until
        .expect("POST has a claimed job");
    let replacement_claim = old_claim + chrono::Duration::minutes(1);
    let changed = fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE pin_jobs SET locked_until=$1 WHERE id=$2 AND locked_until=$3",
            [
                replacement_claim.into(),
                fixture.job.id.clone().into(),
                old_claim.into(),
            ],
        ))
        .await
        .unwrap();
    assert_eq!(changed.rows_affected(), 1);
    // A cancelled dispatch still returns its receipt. Only the lost original
    // claim fence, not token cancellation alone, forbids live projection.
    cancellation.cancel();
    fixture.server.boundary.submit.release.add_permits(1);
    let (result, _) = super::super::drain(&mut work, &cancellation, "signal").await;
    assert!(cancellation.is_cancelled());
    if let Err(error) = result {
        let failure = error
            .downcast_ref::<FailureReport>()
            .expect("typed scoped run failure");
        assert!(matches!(failure.reason, FailureReason::Run));
    }
    drop(work);
    let facts = pin_submit_observation::Entity::find()
        .filter(pin_submit_observation::Column::JobId.eq(&fixture.job.id))
        .all(&fixture.db)
        .await
        .unwrap();
    assert_eq!(facts.len(), 1);
    let actual_fact = &facts[0];
    assert_eq!(actual_fact.job_id, fixture.job.id);
    assert_eq!(actual_fact.provider, fixture.job.provider);
    assert_eq!(actual_fact.expected_cid, fixture.job.cid);
    assert_eq!(actual_fact.route, original_invocation.route);
    assert_eq!(actual_fact.remote_epoch, original_invocation.remote_epoch);
    assert_eq!(actual_fact.claim_until, old_claim);
    assert!(actual_fact.observed_at.is_some());
    assert_eq!(actual_fact.outcome, "ReportedQueued");
    assert!(actual_fact.safe_error.is_none());
    let resources: Vec<ipfs_s3_gateway::store::pinning::ledger::submission::ResourceEvidence> =
        serde_json::from_str(&actual_fact.resources).unwrap();
    assert_eq!(resources.len(), 1);
    let resource = &resources[0].resource;
    assert_eq!(resource.request_id, "dispatched-submit");
    assert!(
        ipfs_s3_gateway::pinning::provider::cids_equivalent(&resource.cid, &fixture.job.cid)
            .unwrap()
    );
    assert_eq!(
        resource.status,
        ipfs_s3_gateway::pinning::provider::ObservedResourceStatus::Reported
    );
    assert_eq!(
        resource.ownership,
        ipfs_s3_gateway::pinning::identity::Ownership::Unknown
    );
    let route: ipfs_s3_gateway::pinning::identity::ProviderRouteSnapshot =
        serde_json::from_str(&original_invocation.route).unwrap();
    let key = resources[0]
        .key
        .as_ref()
        .expect("valid returned CID has a frozen resource key");
    assert_eq!(key.backend, route.backend);
    assert_eq!(key.scope, route.scope);
    assert_eq!(key.resource_type, resource.resource_type);
    assert_eq!(
        key.resource_id,
        ipfs_s3_gateway::pinning::provider::canonical_resource_cid(&resource.cid).unwrap()
    );
    let replica_facts = pin_replica_observation::Entity::find()
        .filter(pin_replica_observation::Column::JobId.eq(&fixture.job.id))
        .filter(pin_replica_observation::Column::Phase.eq("submit"))
        .all(&fixture.db)
        .await
        .unwrap();
    assert_eq!(replica_facts.len(), 1);
    let replica = &replica_facts[0];
    assert_eq!(replica.provider, actual_fact.provider);
    assert_eq!(replica.expected_cid, actual_fact.expected_cid);
    assert_eq!(replica.route, actual_fact.route);
    assert_eq!(replica.remote_epoch, actual_fact.remote_epoch);
    assert_eq!(replica.claim_until, old_claim);
    assert_eq!(replica.lease_id, fixture.job.lease_id);
    assert_eq!(replica.target_id, fixture.job.target_id);
    assert_eq!(replica.expected_generation, fixture.job.expected_generation);
    assert_eq!(replica.outcome, actual_fact.outcome);
    assert!(!replica.projectable);
    let replica_resources: Vec<ipfs_s3_gateway::pinning::provider::ObservedResource> =
        serde_json::from_str(&replica.resources).unwrap();
    assert_eq!(replica_resources.len(), 1);
    assert_eq!(replica_resources[0].request_id, resource.request_id);
    assert_eq!(replica_resources[0].cid, resource.cid);
    assert_eq!(replica_resources[0].status, resource.status);
    assert_eq!(replica_resources[0].ownership, resource.ownership);
    assert_eq!(
        pin_job::Entity::find_by_id(fixture.job.id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .locked_until,
        Some(replacement_claim)
    );
    let binding =
        records::load_confirmed_from_imported(&fixture.db, &fixture.auth, &fixture.imported)
            .await
            .unwrap();
    let committed = records::lookup_committed(&fixture.db, &fixture.auth, &binding, "object")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(committed.receipt.job_ids, vec![fixture.job.id.clone()]);
    fixture.assert_outside_unchanged().await;
    let remote =
        remote_pin::Entity::find_by_id((fixture.job.provider.clone(), fixture.job.cid.clone()))
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
    assert!(remote.request_id.is_none());
    let jobs = pin_job::Entity::find().all(&fixture.db).await.unwrap();
    assert_eq!(jobs.len(), 2);
    assert!(!jobs.iter().any(|job| job.operation == "poll"));
    let tx = fixture.db.begin().await.unwrap();
    tx.execute_unprepared("UPDATE pin_provider_usage SET reserved_pins=reserved_pins")
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), fixture.db.clone().close())
        .await
        .unwrap()
        .unwrap();
    fixture.server.close().await;
    let requests = fixture.server.requests();
    let provider_calls = requests
        .iter()
        .filter(|request| request.path.starts_with("/psa/"))
        .collect::<Vec<_>>();
    assert_eq!(provider_calls.len(), 1);
    assert_eq!(provider_calls[0].path, "/psa/pins");
    assert_eq!(provider_calls[0].method, axum::http::Method::POST);
    assert!(
        ipfs_s3_gateway::pinning::provider::cids_equivalent(
            provider_calls[0].body.as_ref().unwrap()["cid"]
                .as_str()
                .unwrap(),
            &fixture.job.cid
        )
        .unwrap()
    );
}

#[tokio::test]
async fn dropping_apply_run_during_source_io_releases_owned_work_without_a_new_post() {
    let mut fixture = Fixture::new(0).await;
    fixture
        .server
        .boundary
        .source
        .armed
        .store(true, Ordering::SeqCst);
    let cancellation = CancellationToken::new();
    let command = Command::Apply {
        confirmed: "in-process-confirmed-document".into(),
        run: true,
    };
    let mut work = Box::pin(execute(
        &fixture.db,
        &fixture.raw,
        &fixture.config,
        &fixture.auth,
        &command,
        &fixture.imported,
        cancellation.clone(),
    ));
    tokio::select! {
        _ = fixture.server.boundary.source.entered.notified() => {},
        result = &mut work => panic!("scoped execution ended before source IO: {result:?}"),
        _ = tokio::time::sleep(Duration::from_secs(10)) => panic!("scoped source IO did not dispatch"),
    }
    cancellation.cancel();
    drop(work);
    fixture.server.boundary.source.release.add_permits(1);
    fixture.assert_outside_unchanged().await;
    let tx = tokio::time::timeout(Duration::from_secs(5), fixture.db.begin())
        .await
        .unwrap()
        .unwrap();
    tx.execute_unprepared("UPDATE pin_provider_usage SET reserved_pins=reserved_pins")
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), fixture.db.clone().close())
        .await
        .unwrap()
        .unwrap();
    fixture.server.close().await;
    let requests = fixture.server.requests();
    assert!(
        !requests
            .iter()
            .any(|request| request.path.starts_with("/psa/"))
    );
}

// Observe the real caller connection; never pass it into the blocking decoder.
struct ObservedTransaction {
    tx: DatabaseTransaction,
    calls: Arc<AtomicUsize>,
    decode_ready: AtomicBool,
}

#[async_trait::async_trait]
impl ConnectionTrait for ObservedTransaction {
    fn get_database_backend(&self) -> DatabaseBackend {
        self.tx.get_database_backend()
    }
    async fn execute(&self, statement: Statement) -> std::result::Result<ExecResult, DbErr> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.tx.execute(statement).await
    }
    async fn execute_unprepared(&self, sql: &str) -> std::result::Result<ExecResult, DbErr> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.tx.execute_unprepared(sql).await
    }
    async fn query_one(
        &self,
        statement: Statement,
    ) -> std::result::Result<Option<QueryResult>, DbErr> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.tx.query_one(statement).await
    }
    async fn query_all(
        &self,
        statement: Statement,
    ) -> std::result::Result<Vec<QueryResult>, DbErr> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let rows = self.tx.query_all(statement).await?;
        if rows.iter().any(|row| {
            row.try_get::<String>("", "binding").is_ok()
                && row.try_get::<String>("", "result").is_ok()
        }) {
            // Complete private source delivery precedes the awaited decoder;
            // the connection itself remains owned by the async caller.
            self.decode_ready.store(true, Ordering::SeqCst);
        }
        Ok(rows)
    }
}

struct ReleaseBlocking(Option<std::sync::mpsc::Sender<()>>);
impl Drop for ReleaseBlocking {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[test]
fn cancelled_private_decode_waiter_rolls_back_and_default_runtime_has_a_finite_tail() {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let runner = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        let (release, blocker, tail, finished, calls, at_cancel) = runtime.block_on(async {
            // The current row, durable original, SQL transfers and parse scratch
            // all fit their cumulative caps. Cancellation uses a controlled CPU
            // boundary, not a payload-size-dependent timing assumption.
            let fixture = Fixture::new(512 * 1024).await;
            // A completed real lookup proves this is accepted original authority,
            // not a malformed private record that exits before the expensive seam.
            assert!(
                job_authority::lookup(&fixture.db, &fixture.job)
                    .await
                    .unwrap()
                    .is_some()
            );
            for decode_completed in [false, true] {
                let usage = fixture
                    .db
                    .query_one(Statement::from_string(
                        fixture.db.get_database_backend(),
                        "SELECT reserved_pins FROM pin_provider_usage",
                    ))
                    .await
                    .unwrap()
                    .unwrap();
                let reserved_before = usage.try_get::<i64>("", "reserved_pins").unwrap();
                let tx = fixture.db.begin().await.unwrap();
                tx.execute_unprepared(
                    "UPDATE pin_provider_usage SET reserved_pins=reserved_pins+1",
                )
                .await
                .unwrap();
                let calls = Arc::new(AtomicUsize::new(0));
                let observed = ObservedTransaction {
                    tx,
                    calls: calls.clone(),
                    decode_ready: AtomicBool::new(false),
                };
                let (release_tx, release_rx) = std::sync::mpsc::channel();
                let release = ReleaseBlocking(Some(release_tx));
                let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                let blocker = tokio::task::spawn_blocking(move || {
                    let _ = started_tx.send(());
                    let _ = release_rx.recv();
                });
                started_rx.await.unwrap();
                let mut lookup = Box::pin(job_authority::lookup(&observed, &fixture.job));
                poll_fn(|context| match Pin::as_mut(&mut lookup).poll(context) {
                    Poll::Ready(_) => {
                        panic!("authority completed before the controlled CPU boundary")
                    }
                    Poll::Pending if observed.decode_ready.load(Ordering::SeqCst) => {
                        Poll::Ready(())
                    }
                    Poll::Pending => Poll::Pending,
                })
                .await;
                // With one worker this sentinel can only finish after the real
                // private decode. Do not poll its waiting authority consumer.
                let tail = tokio::task::spawn_blocking(|| ());
                let at_cancel = calls.load(Ordering::SeqCst);
                if decode_completed {
                    drop(release);
                    blocker.await.unwrap();
                    tail.await.unwrap();
                    drop(lookup);
                    drop(observed);
                } else {
                    drop(lookup);
                    drop(observed); // rollback need not wait for readonly CPU
                    drop(release);
                    blocker.await.unwrap();
                    tail.await.unwrap();
                }
                let reopened = store::connect_database(&fixture.raw.storage.database_url)
                    .await
                    .unwrap();
                let tx = tokio::time::timeout(Duration::from_secs(5), reopened.begin())
                    .await
                    .unwrap()
                    .unwrap();
                tx.execute_unprepared("UPDATE pin_provider_usage SET reserved_pins=reserved_pins")
                    .await
                    .unwrap();
                let usage = tx
                    .query_one(Statement::from_string(
                        tx.get_database_backend(),
                        "SELECT reserved_pins FROM pin_provider_usage",
                    ))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    usage.try_get::<i64>("", "reserved_pins").unwrap(),
                    reserved_before
                );
                tx.rollback().await.unwrap();
                reopened.close().await.unwrap();
                assert_eq!(calls.load(Ordering::SeqCst), at_cancel);
                fixture.assert_outside_unchanged().await;
            }
            // Leave one real private decoder and a completion sentinel queued
            // at shutdown. The decoder owns row bytes, not the caller or DB.
            let tx = fixture.db.begin().await.unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let observed = ObservedTransaction {
                tx,
                calls: calls.clone(),
                decode_ready: AtomicBool::new(false),
            };
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let release = ReleaseBlocking(Some(release_tx));
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                let _ = started_tx.send(());
                let _ = release_rx.recv();
            });
            started_rx.await.unwrap();
            let mut lookup = Box::pin(job_authority::lookup(&observed, &fixture.job));
            poll_fn(|context| match lookup.as_mut().poll(context) {
                Poll::Ready(_) => panic!("authority completed before the controlled CPU boundary"),
                Poll::Pending if observed.decode_ready.load(Ordering::SeqCst) => Poll::Ready(()),
                Poll::Pending => Poll::Pending,
            })
            .await;
            let finished = Arc::new(AtomicBool::new(false));
            let completion = finished.clone();
            let tail =
                tokio::task::spawn_blocking(move || completion.store(true, Ordering::SeqCst));
            let at_cancel = calls.load(Ordering::SeqCst);
            drop(lookup);
            drop(observed);
            fixture.db.clone().close().await.unwrap();
            drop(fixture.directory);
            (release, blocker, tail, finished, calls, at_cancel)
        });
        drop(release);
        drop(blocker);
        drop(tail);
        drop(runtime); // ordinary shutdown waits for the unconsumed CPU tail
        assert!(finished.load(Ordering::SeqCst));
        assert_eq!(calls.load(Ordering::SeqCst), at_cancel);
        done_tx.send(()).unwrap();
    });
    done_rx.recv_timeout(Duration::from_secs(30)).expect(
        "legal private decoding and default runtime shutdown exceeded the finite tail budget",
    );
    runner.join().unwrap();
}
