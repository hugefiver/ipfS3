//! Deterministic network gates exercise the actual scheduler, never fabricated claims.
use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    http::StatusCode,
    routing::post,
};
use chrono::Utc;
use futures_util::FutureExt;
use http_body_util::BodyExt;
use ipfs_s3_gateway::{
    config::Config,
    kubo::KuboClient,
    pinning::{
        config::{LeaseDuration, ProviderMode, ValidatedPinningConfig},
        coordinator::PinningCoordinator,
        policy::{LeaseIntent, LeaseSource, PublicationPolicy},
        tags::ContentMode,
    },
    store::{
        self, Store,
        entities::{
            pin_invocation_route, pin_job, pin_lease, pin_lease_target, pin_provider_route,
            pin_provider_usage, pin_resource_history, remote_pin,
        },
        pinning::{
            jobs, leases, ledger,
            publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
            quota,
        },
    },
};
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection,
    DbErr, EntityTrait, ExecResult, QueryFilter, QueryResult, Set, Statement, TransactionTrait,
    sea_query::Expr,
};
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const OTHER: &str = "bafkreigh2akiscaildc6ii5zji4bq7kly5k3s7svv6q2wx2nn5rtj5xuu4";

#[derive(Clone)]
struct TargetState {
    started: Arc<Notify>,
    release: Arc<Semaphore>,
    adds: Arc<AtomicUsize>,
    reads: Arc<AtomicUsize>,
    unexpected: Arc<AtomicUsize>,
    first_response: (StatusCode, String),
}

async fn add(State(state): State<TargetState>, mut body: Body) -> (StatusCode, String) {
    while let Some(frame) = body.frame().await {
        frame.unwrap();
    }
    let call = state.adds.fetch_add(1, Ordering::SeqCst);
    if call == 0 {
        state.started.notify_one();
        state.release.acquire().await.unwrap().forget();
        state.first_response
    } else {
        (StatusCode::OK, format!("{{\"Hash\":\"{OTHER}\"}}\n"))
    }
}

async fn pin_ls(
    State(state): State<TargetState>,
    Query(query): Query<HashMap<String, String>>,
) -> String {
    state.reads.fetch_add(1, Ordering::SeqCst);
    let cid = query.get("arg").unwrap();
    serde_json::json!({"Keys":{(cid):{"Type":"recursive"}}}).to_string()
}

struct Fixture {
    store: Store,
    coordinator: Arc<PinningCoordinator>,
    provider: String,
    target: TargetState,
    server: tokio::task::JoinHandle<()>,
    _source: MockServer,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.target.release.add_permits(1);
        self.server.abort();
    }
}

impl Fixture {
    async fn new(first_response: (StatusCode, String)) -> Self {
        let db = store::connect_database("sqlite::memory:").await.unwrap();
        Self::with_database(first_response, db).await
    }

    async fn with_database(first_response: (StatusCode, String), db: DatabaseConnection) -> Self {
        let source = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/files/stat"))
            .respond_with(|request: &wiremock::Request| {
                let cid = request
                    .url
                    .query_pairs()
                    .find(|(key, _)| key == "arg")
                    .unwrap()
                    .1
                    .trim_start_matches("/ipfs/")
                    .to_owned();
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"Hash":cid,"Type":"file"}))
            })
            .mount(&source)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"stored-object"))
            .mount(&source)
            .await;
        let target = TargetState {
            started: Arc::new(Notify::new()),
            release: Arc::new(Semaphore::new(0)),
            adds: Arc::new(AtomicUsize::new(0)),
            reads: Arc::new(AtomicUsize::new(0)),
            unexpected: Arc::new(AtomicUsize::new(0)),
            first_response,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        // No DELETE or pin/rm route exists; any unexpected request also fails the leaf operation.
        let app = Router::new()
            .route("/api/v0/add", post(add))
            .route("/api/v0/pin/ls", post(pin_ls))
            .with_state(target.clone())
            .layer(axum::middleware::from_fn_with_state(
                target.clone(),
                |State(state): State<TargetState>,
                 request: axum::extract::Request,
                 next: axum::middleware::Next| async move {
                    if request.method() != "POST"
                        || !matches!(request.uri().path(), "/api/v0/add" | "/api/v0/pin/ls")
                    {
                        state.unexpected.fetch_add(1, Ordering::SeqCst);
                    }
                    next.run(request).await
                },
            ));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let raw: Config = toml::from_str(&format!(
            r#"
            [pinning]
            worker_interval = '1s'
            [[pinning.providers]]
            name = 'rpc'
            kind = 'filebase'
            api = 'rpc'
            endpoint = '{endpoint}'
            strategy = 'upload'
            token_env = 'FIXTURE_SECRET'
            priority = 1
            max_bytes = 1000
            max_pins = 10
            [[pinning_rpc.providers]]
            config_name = 'rpc'
            profile = 'filebase'
            auth = 'bearer'
            allow_private_network = true
            control_timeout_seconds = 20
            idle_timeout_seconds = 20
            [pinning_identity]
            primary_storage_domain = 'local'
            [[pinning_identity.providers]]
            config_name = 'rpc'
            provider_id = 'rpc'
            display_name = 'RPC fixture'
            backend = 'filebase'
            scope = 'availability-node'
            storage_domain = 'remote'
            credential_revision = 1
            endpoint_revision = 1
            secret_ref = 'env:FIXTURE_SECRET'
            api_profile = 'filebase-rpc'
            strategy = 'upload'
            cleanup = 'managed'
        "#
        ))
        .unwrap();
        let coordinator = PinningCoordinator::build_with_kubo(
            ValidatedPinningConfig::from_config(&raw, |_| Some("synthetic-token".into())).unwrap(),
            Some(KuboClient::new(source.uri())),
        )
        .unwrap();
        store::run_migrations(&db).await.unwrap();
        store::bucket::create(&db, "availability", None)
            .await
            .unwrap();
        let store = Store::new(db);
        coordinator.register_identities(&store).await.unwrap();
        let provider = coordinator.provider_limits().keys().next().unwrap().clone();
        Self {
            store,
            coordinator,
            provider,
            target,
            server,
            _source: source,
        }
    }

    async fn publish(&self, key: &str, cid: &str) -> String {
        let object_id = uuid::Uuid::new_v4().to_string();
        publication::publish_object(
            self.store.db(),
            PublicationRequest {
                object: PublicationObject::from_put(
                    object_id.clone(),
                    "availability",
                    key,
                    cid.into(),
                    100,
                    None,
                    None,
                    false,
                    None,
                    None,
                    Utc::now(),
                ),
                tags: vec![],
                policy: PublicationPolicy {
                    tags: vec![],
                    leases: vec![LeaseIntent {
                        source: LeaseSource::Automatic,
                        policy_id: "availability".into(),
                        provider_mode: ProviderMode::All,
                        providers: vec![self.provider.clone()],
                        content_mode: ContentMode::Object,
                        duration: LeaseDuration::parse("1h").unwrap(),
                    }],
                },
                object_target: PinTargetSpec {
                    cid: cid.into(),
                    logical_size: 100,
                },
            },
            self.coordinator.provider_limits(),
        )
        .await
        .unwrap();
        let lease = store::entities::pin_lease::Entity::find()
            .filter(store::entities::pin_lease::Column::OwnerObjectId.eq(object_id))
            .one(self.store.db())
            .await
            .unwrap()
            .unwrap();
        pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq(lease.id))
            .one(self.store.db())
            .await
            .unwrap()
            .unwrap()
            .id
    }

    async fn target_state(&self, id: &str) -> String {
        pin_lease_target::Entity::find_by_id(id.to_owned())
            .one(self.store.db())
            .await
            .unwrap()
            .unwrap()
            .state
    }
    async fn submit(&self) -> pin_job::Model {
        pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .filter(pin_job::Column::Cid.eq(CID))
            .one(self.store.db())
            .await
            .unwrap()
            .unwrap()
    }
    async fn receipt(&self) -> ledger::submission_entity::Model {
        ledger::submission::latest(self.store.db(), &self.submit().await.id)
            .await
            .unwrap()
            .unwrap()
    }
    async fn held(&self, bytes: i64, pins: i64) {
        let usage = pin_provider_usage::Entity::find_by_id(self.provider.clone())
            .one(self.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (bytes, pins));
    }
    fn assert_posts(&self, count: usize) {
        assert_eq!(self.target.adds.load(Ordering::SeqCst), count);
        assert_eq!(
            self.target.unexpected.load(Ordering::SeqCst),
            0,
            "no DELETE, pin/rm or fallback I/O is allowed"
        );
    }

    async fn takeover_park(&self) -> pin_job::Model {
        let original = self.submit().await;
        let expired = pin_job::Entity::update_many()
            .col_expr(
                pin_job::Column::LockedUntil,
                Expr::value(Utc::now() - chrono::Duration::seconds(1)),
            )
            .filter(pin_job::Column::Id.eq(&original.id))
            .filter(pin_job::Column::LockedUntil.eq(original.locked_until.unwrap()))
            .exec(self.store.db())
            .await
            .unwrap();
        assert_eq!(expired.rows_affected, 1);
        wait_for_stage("takeover parked recovery", || async {
            let job = self.submit().await;
            job.state == "running"
                && job.locked_until.is_none()
                && job.submit_phase.as_deref() == Some("recovering")
        })
        .await;
        assert_eq!(self.receipt().await.outcome, "in_flight");
        original
    }
}

async fn wait_for<F: Future<Output = bool>, C: FnMut() -> F>(predicate: C) {
    wait_for_stage("expected durable state", predicate).await;
}

async fn wait_for_stage<F: Future<Output = bool>, C: FnMut() -> F>(stage: &str, mut predicate: C) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !predicate().await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("scheduler failed to reach {stage} within 10s"));
}

async fn held_start(fixture: &Fixture) {
    tokio::time::timeout(Duration::from_secs(5), fixture.target.started.notified())
        .await
        .expect("A did not dispatch");
    assert_eq!(fixture.receipt().await.outcome, "in_flight");
}

/// The read completes before the gate: the other connection really commits a
/// new database state, rather than returning invented rows or sleeps/race odds.
struct AdmissionReadGate {
    inner: DatabaseConnection,
    pause_on: usize,
    after_read: bool,
    reads: AtomicUsize,
    entered: Notify,
    resume: Semaphore,
}

#[async_trait::async_trait]
impl ConnectionTrait for AdmissionReadGate {
    fn get_database_backend(&self) -> DatabaseBackend {
        self.inner.get_database_backend()
    }
    async fn execute(&self, statement: Statement) -> Result<ExecResult, DbErr> {
        self.inner.execute(statement).await
    }
    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        self.inner.execute_unprepared(sql).await
    }
    async fn query_one(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        self.inner.query_one(statement).await
    }
    async fn query_all(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        let pause = statement.sql.contains("pin_submit_observations")
            && self.reads.fetch_add(1, Ordering::SeqCst) + 1 == self.pause_on;
        if pause && !self.after_read {
            self.entered.notify_one();
            self.resume.acquire().await.unwrap().forget();
        }
        let result = self.inner.query_all(statement).await;
        if pause && self.after_read {
            self.entered.notify_one();
            self.resume.acquire().await.unwrap().forget();
        }
        result
    }
}

async fn file_fixture() -> (tempfile::TempDir, Fixture, DatabaseConnection) {
    let root = std::env::temp_dir();
    let approved = root.join("opencode");
    let dir = tempfile::Builder::new()
        .prefix("rpc-admission-")
        .tempdir_in(if approved.is_dir() { approved } else { root })
        .unwrap();
    let file = dir
        .path()
        .join("availability.db")
        .to_string_lossy()
        .replace('\\', "/");
    let url = format!("sqlite://{file}?mode=rwc");
    let db = store::connect_database(&url).await.unwrap();
    db.execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    let fixture =
        Fixture::with_database((StatusCode::OK, format!("{{\"Hash\":\"{CID}\"}}\n")), db).await;
    let peer = store::connect_database(&url).await.unwrap();
    (dir, fixture, peer)
}

fn admission_gate(
    inner: DatabaseConnection,
    pause_on: usize,
    after_read: bool,
) -> AdmissionReadGate {
    AdmissionReadGate {
        inner,
        pause_on,
        after_read,
        reads: AtomicUsize::new(0),
        entered: Notify::new(),
        resume: Semaphore::new(0),
    }
}

#[tokio::test]
async fn stable_operator_waiter_reason_survives_debt_settled_after_reserve_return() {
    let (_dir, fixture, peer) = file_fixture().await;
    let a = fixture.publish("a", CID).await;
    let worker = fixture
        .coordinator
        .start(fixture.store.clone(), CancellationToken::new());
    held_start(&fixture).await;
    let b = fixture.publish("b", OTHER).await;
    // Keep the periodic scanner out of B; this exact caller controls its wake.
    let wake_at = Utc::now() + chrono::Duration::seconds(30);
    pin_lease_target::Entity::update_many()
        .col_expr(
            pin_lease_target::Column::LastTouchedAt,
            Expr::value(wake_at),
        )
        .filter(pin_lease_target::Column::Id.eq(&b))
        .exec(&peer)
        .await
        .unwrap();
    fixture.takeover_park().await;
    assert_eq!(
        ledger::submission::admission_barrier(&peer, &fixture.provider)
            .await
            .unwrap(),
        ledger::submission::AdmissionBarrier::Operator
    );
    assert_eq!(
        quota::reserve_unique(
            &peer,
            &fixture.provider,
            OTHER,
            100,
            fixture.coordinator.provider_limits(),
            wake_at
        )
        .await
        .unwrap(),
        quota::ReservationOutcome::QuotaBlocked
    );
    let gate = admission_gate(peer.clone(), 2, false);
    let wake = quota::wake_provider_waiters(
        &gate,
        &fixture.provider,
        &fixture.coordinator.provider_limits()[&fixture.provider],
        wake_at,
    );
    tokio::pin!(wake);
    let result = tokio::select! {
        _ = gate.entered.notified() => {
            // Old code has returned operator QuotaBlocked and now starts its
            // independent has_debt read. Complete A before that second read.
            fixture.target.release.add_permits(1);
            wait_for(|| async { fixture.target_state(&a).await == "pinned" && fixture.submit().await.state == "done" && !fixture.receipt().await.needs_attention }).await;
            assert!(!ledger::submission::has_debt(fixture.store.db(), &fixture.provider).await.unwrap());
            gate.resume.add_permits(1);
            wake.await
        }
        result = &mut wake => {
            // Stable waiter-specific classification needs no second debt read.
            assert_eq!(gate.reads.load(Ordering::SeqCst), 1);
            fixture.target.release.add_permits(1);
            wait_for(|| async { fixture.target_state(&a).await == "pinned" && fixture.submit().await.state == "done" && !fixture.receipt().await.needs_attention }).await;
            result
        }
    };
    assert!(result.unwrap().is_empty());
    worker.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        fixture.target_state(&b).await,
        "quota_waiting",
        "the original operator-block reason must keep this waiter recoverable even after debt clears"
    );
    fixture.held(100, 1).await;
    fixture.assert_posts(1);
    let txn = peer.begin().await.unwrap();
    let woken = quota::wake_provider_waiters(
        &txn,
        &fixture.provider,
        &fixture.coordinator.provider_limits()[&fixture.provider],
        wake_at,
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    assert_eq!(woken, vec![b.clone()]);
    assert_eq!(fixture.target_state(&b).await, "waiting");
    fixture.held(200, 2).await;
    peer.close().await.unwrap();
    fixture.store.db().clone().close().await.unwrap();
    _dir.close().unwrap();
}

#[tokio::test]
async fn stable_admission_snapshot_old_inflight_and_new_done_never_become_operator() {
    let (_dir, fixture, peer) = file_fixture().await;
    let a = fixture.publish("a", CID).await;
    let worker = fixture
        .coordinator
        .start(fixture.store.clone(), CancellationToken::new());
    held_start(&fixture).await;
    let gate = admission_gate(peer.clone(), 1, true);
    let reserve = quota::reserve_unique(
        &gate,
        &fixture.provider,
        OTHER,
        100,
        fixture.coordinator.provider_limits(),
        Utc::now(),
    );
    tokio::pin!(reserve);
    tokio::select! {
        _ = gate.entered.notified() => {},
        result = &mut reserve => panic!("old evidence gate was bypassed: {result:?}"),
    }
    // Old evidence has been read, with no transaction/write lock preventing
    // this independently connected real worker from committing every new state.
    assert_eq!(fixture.receipt().await.outcome, "in_flight");
    fixture.target.release.add_permits(1);
    wait_for(|| async {
        fixture.target_state(&a).await == "pinned"
            && fixture.submit().await.state == "done"
            && !fixture.receipt().await.needs_attention
    })
    .await;
    assert_eq!(fixture.receipt().await.outcome, "matched");
    gate.resume.add_permits(1);
    let result = reserve.await.unwrap();
    assert!(
        matches!(result, quota::ReservationOutcome::QuotaWaiting { .. }),
        "old healthy snapshot must stay temporary, not mix in new done state: {result:?}"
    );
    worker.shutdown(Duration::from_secs(2)).await;
    fixture.held(100, 1).await;
    fixture.assert_posts(1);
    // A first publication after the consistent read uses the current healthy
    // state; the mixed read never creates a permanent blocked target.
    let b = fixture.publish("b", OTHER).await;
    assert_eq!(fixture.target_state(&b).await, "waiting");
    fixture.held(200, 2).await;
    peer.close().await.unwrap();
    fixture.store.db().clone().close().await.unwrap();
    _dir.close().unwrap();
}

#[tokio::test]
async fn healthy_held_a_different_cid_b_automatically_resumes() {
    let fixture = Fixture::new((StatusCode::OK, format!("{{\"Hash\":\"{CID}\"}}\n"))).await;
    let a = fixture.publish("a", CID).await;
    let worker = fixture
        .coordinator
        .start(fixture.store.clone(), CancellationToken::new());
    held_start(&fixture).await;
    let b = fixture.publish("b", OTHER).await;
    assert_eq!(
        fixture.target_state(&b).await,
        "quota_waiting",
        "healthy in-flight debt must not permanently block B"
    );
    fixture.held(100, 1).await;
    fixture.target.release.add_permits(1);
    wait_for(|| async {
        fixture.target_state(&a).await == "pinned" && fixture.target_state(&b).await == "pinned"
    })
    .await;
    worker.shutdown(Duration::from_secs(2)).await;
    fixture.held(200, 2).await;
    fixture.assert_posts(2);
    assert!(
        !ledger::submission::has_debt(fixture.store.db(), &fixture.provider)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn healthy_held_a_same_cid_b_reuses_without_second_post() {
    let fixture = Fixture::new((StatusCode::OK, format!("{{\"Hash\":\"{CID}\"}}\n"))).await;
    let a = fixture.publish("a", CID).await;
    let worker = fixture
        .coordinator
        .start(fixture.store.clone(), CancellationToken::new());
    held_start(&fixture).await;
    let b = fixture.publish("b", CID).await;
    assert_eq!(fixture.target_state(&b).await, "quota_waiting");
    fixture.target.release.add_permits(1);
    wait_for(|| async {
        fixture.target_state(&a).await == "pinned" && fixture.target_state(&b).await == "pinned"
    })
    .await;
    worker.shutdown(Duration::from_secs(2)).await;
    fixture.held(100, 1).await;
    fixture.assert_posts(1);
}

#[tokio::test]
async fn takeover_park_then_late_complete_receipt_is_replayed_by_new_claim() {
    let fixture = Fixture::new((StatusCode::OK, format!("{{\"Hash\":\"{CID}\"}}\n"))).await;
    let target = fixture.publish("a", CID).await;
    fixture.store.db().execute_unprepared("CREATE TABLE recovery_claim_audit(job_id TEXT, locked_until TEXT);
        CREATE TRIGGER audit_fresh_recovery AFTER UPDATE OF locked_until ON pin_jobs
        WHEN OLD.state='pending' AND NEW.state='running' AND NEW.submit_phase='recovering' AND OLD.locked_until IS NULL AND NEW.locked_until IS NOT NULL
        BEGIN INSERT INTO recovery_claim_audit VALUES(NEW.id, NEW.locked_until); END;
        CREATE TRIGGER forbid_old_caller_projection BEFORE UPDATE OF status ON remote_pins
        WHEN NEW.status='pinned' AND NOT EXISTS(SELECT 1 FROM pin_jobs WHERE provider=NEW.provider AND cid=NEW.cid AND operation='submit' AND state='running' AND submit_phase='recovering' AND locked_until IS NOT NULL)
        BEGIN SELECT RAISE(ABORT,'fresh recovering claim required'); END;").await.unwrap();
    let worker = fixture
        .coordinator
        .start(fixture.store.clone(), CancellationToken::new());
    held_start(&fixture).await;
    let original = fixture.takeover_park().await;
    fixture.target.release.add_permits(1);
    wait_for(|| async {
        fixture.target_state(&target).await == "pinned" && fixture.submit().await.state == "done"
    })
    .await;
    worker.shutdown(Duration::from_secs(2)).await;
    fixture.held(100, 1).await;
    fixture.assert_posts(1);
    assert_eq!(
        fixture.target.reads.load(Ordering::SeqCst),
        1,
        "no expected-CID recovery query is allowed"
    );
    assert!(!fixture.receipt().await.needs_attention);
    assert_eq!(
        jobs::submission_history(fixture.store.db(), &original.id)
            .await
            .unwrap()
            .unwrap()
            .submit_calls,
        1
    );
    let audit = fixture
        .store
        .db()
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS count FROM recovery_claim_audit".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        audit.try_get::<i64>("", "count").unwrap(),
        1,
        "a new real recovery claim, not the old caller, must project"
    );
}

#[tokio::test]
async fn unknown_a_keeps_waiter_recoverable_but_never_grants_capacity_or_reposts() {
    let fixture = Fixture::new((StatusCode::FORBIDDEN, String::new())).await;
    fixture.publish("a", CID).await;
    let worker = fixture
        .coordinator
        .start(fixture.store.clone(), CancellationToken::new());
    held_start(&fixture).await;
    let b = fixture.publish("b", OTHER).await;
    assert_eq!(fixture.target_state(&b).await, "quota_waiting");
    fixture.target.release.add_permits(1);
    wait_for(|| async { fixture.receipt().await.outcome == "unknown" }).await;
    for _ in 0..3 {
        let txn = fixture.store.db().begin().await.unwrap();
        let limits = &fixture.coordinator.provider_limits()[&fixture.provider];
        assert!(
            quota::wake_provider_waiters(&txn, &fixture.provider, limits, Utc::now())
                .await
                .unwrap()
                .is_empty()
        );
        txn.commit().await.unwrap();
        assert_eq!(
            fixture.target_state(&b).await,
            "quota_waiting",
            "operator debt may not turn a recoverable waiter permanently blocked"
        );
    }
    worker.shutdown(Duration::from_secs(2)).await;
    fixture.held(100, 1).await;
    assert!(fixture.receipt().await.needs_attention);
    assert!(
        ledger::submission::has_debt(fixture.store.db(), &fixture.provider)
            .await
            .unwrap()
    );
    fixture.assert_posts(1);
    assert_eq!(fixture.target.reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn late_matched_receipt_cannot_wake_changed_history_identity_or_zero_desired() {
    for boundary in [
        "credential",
        "endpoint",
        "capture_epoch",
        "archived_lifetime",
        "generation",
        "zero_desired",
        "request",
        "history_call",
        "history_strategy",
    ] {
        let fixture = Fixture::new((StatusCode::OK, format!("{{\"Hash\":\"{CID}\"}}\n"))).await;
        let target = fixture.publish("a", CID).await;
        let worker = fixture
            .coordinator
            .start(fixture.store.clone(), CancellationToken::new());
        held_start(&fixture).await;
        let original = fixture.takeover_park().await;
        let receipt = fixture.receipt().await;
        match boundary {
            "credential" | "endpoint" => {
                let row = pin_provider_route::Entity::find_by_id(fixture.provider.clone())
                    .one(fixture.store.db())
                    .await
                    .unwrap()
                    .unwrap();
                let mut route: serde_json::Value = serde_json::from_str(&row.snapshot).unwrap();
                route[format!("{boundary}_revision")] = 2.into();
                pin_provider_route::Entity::update_many()
                    .col_expr(
                        pin_provider_route::Column::Snapshot,
                        Expr::value(route.to_string()),
                    )
                    .filter(pin_provider_route::Column::Provider.eq(&fixture.provider))
                    .exec(fixture.store.db())
                    .await
                    .unwrap();
            }
            "capture_epoch" => {
                pin_invocation_route::Entity::update_many()
                    .col_expr(
                        pin_invocation_route::Column::RemoteEpoch,
                        Expr::value(receipt.remote_epoch + 1),
                    )
                    .filter(pin_invocation_route::Column::JobId.eq(&original.id))
                    .exec(fixture.store.db())
                    .await
                    .unwrap();
            }
            "archived_lifetime" => {
                let ledger = ledger::get(fixture.store.db(), &fixture.provider, CID)
                    .await
                    .unwrap()
                    .unwrap();
                pin_resource_history::Entity::insert(pin_resource_history::ActiveModel {
                    provider: Set(fixture.provider.clone()),
                    cid: Set(CID.into()),
                    epoch: Set(receipt.remote_epoch),
                    ledger: Set(serde_json::to_string(&ledger).unwrap()),
                })
                .exec(fixture.store.db())
                .await
                .unwrap();
                remote_pin::Entity::update_many()
                    .col_expr(
                        remote_pin::Column::Epoch,
                        Expr::value(receipt.remote_epoch + 1),
                    )
                    .filter(remote_pin::Column::Provider.eq(&fixture.provider))
                    .filter(remote_pin::Column::Cid.eq(CID))
                    .exec(fixture.store.db())
                    .await
                    .unwrap();
            }
            "generation" => {
                pin_lease::Entity::update_many()
                    .col_expr(
                        pin_lease::Column::Generation,
                        Expr::value(original.expected_generation.unwrap() + 1),
                    )
                    .filter(pin_lease::Column::Id.eq(original.lease_id.clone().unwrap()))
                    .exec(fixture.store.db())
                    .await
                    .unwrap();
            }
            "zero_desired" => {
                let txn = fixture.store.db().begin().await.unwrap();
                leases::cancel_lease(&txn, original.lease_id.as_deref().unwrap(), Utc::now())
                    .await
                    .unwrap();
                txn.commit().await.unwrap();
            }
            "request" => {
                remote_pin::Entity::update_many()
                    .col_expr(
                        remote_pin::Column::RequestId,
                        Expr::value(Some("different-resource-receipt")),
                    )
                    .filter(remote_pin::Column::Provider.eq(&fixture.provider))
                    .filter(remote_pin::Column::Cid.eq(CID))
                    .exec(fixture.store.db())
                    .await
                    .unwrap();
            }
            "history_call" => {
                jobs::history::Entity::update_many()
                    .col_expr(
                        jobs::history::Column::SubmitCalls,
                        Expr::value(receipt.submit_call + 1),
                    )
                    .filter(jobs::history::Column::JobId.eq(&original.id))
                    .exec(fixture.store.db())
                    .await
                    .unwrap();
            }
            "history_strategy" => {
                jobs::history::Entity::update_many()
                    .col_expr(jobs::history::Column::Strategy, Expr::value("cid"))
                    .filter(jobs::history::Column::JobId.eq(&original.id))
                    .exec(fixture.store.db())
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        fixture.target.release.add_permits(1);
        wait_for(|| async { fixture.receipt().await.observed_at.is_some() }).await;
        worker.shutdown(Duration::from_secs(2)).await;
        let job = fixture.submit().await;
        assert_eq!(
            job.state, "running",
            "{boundary} must not queue new recovery"
        );
        assert!(job.locked_until.is_none(), "{boundary}");
        assert_ne!(fixture.target_state(&target).await, "pinned", "{boundary}");
        let receipt = fixture.receipt().await;
        assert_eq!(
            receipt.outcome, "matched",
            "actual complete evidence remains independently queryable"
        );
        assert!(receipt.needs_attention, "{boundary} cannot clear debt");
        assert!(
            ledger::submission::has_debt(fixture.store.db(), &fixture.provider)
                .await
                .unwrap()
        );
        fixture.held(100, 1).await;
        fixture.assert_posts(1);
        assert_eq!(fixture.target.reads.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn complete_receipt_before_takeover_parks_is_not_stranded_and_waiters_stay_recoverable() {
    let fixture = Fixture::new((StatusCode::OK, format!("{{\"Hash\":\"{CID}\"}}\n"))).await;
    let a = fixture.publish("a", CID).await;
    let worker = fixture
        .coordinator
        .start(fixture.store.clone(), CancellationToken::new());
    held_start(&fixture).await;
    let original = fixture.submit().await;
    // Expire + acquire the real B claim atomically so the scheduler cannot park
    // B before A's receipt. This fixes the opposite interleaving deterministically.
    let txn = fixture.store.db().begin().await.unwrap();
    pin_job::Entity::update_many()
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::value(Utc::now() - chrono::Duration::seconds(1)),
        )
        .filter(pin_job::Column::Id.eq(&original.id))
        .filter(pin_job::Column::LockedUntil.eq(original.locked_until.unwrap()))
        .exec(&txn)
        .await
        .unwrap();
    let takeover = jobs::claim_due_jobs(&txn, Utc::now(), chrono::Duration::seconds(30), 32)
        .await
        .unwrap()
        .into_iter()
        .find(|claim| claim.model.id == original.id)
        .unwrap();
    txn.commit().await.unwrap();
    assert!(takeover.reclaimed);
    assert_eq!(takeover.previous_state, "running");
    assert!(takeover.object_id.is_some());
    fixture.target.release.add_permits(1);
    wait_for(|| async { fixture.receipt().await.outcome == "matched" }).await;
    assert!(fixture.receipt().await.needs_attention);
    assert_eq!(
        fixture.submit().await.locked_until,
        takeover.model.locked_until,
        "old receipt may not replace B's live claim"
    );
    assert_ne!(fixture.target_state(&a).await, "pinned");
    let b = fixture.publish("b", OTHER).await;
    assert_eq!(
        fixture.target_state(&b).await,
        "quota_waiting",
        "complete but unprojected evidence is temporary, not operator debt"
    );
    jobs::park_submit(
        fixture.store.db(),
        &takeover,
        "needs_attention",
        "RPC side effects require operator attention",
        Utc::now(),
    )
    .await
    .unwrap();
    wait_for(|| async {
        fixture.target_state(&a).await == "pinned" && fixture.target_state(&b).await == "pinned"
    })
    .await;
    worker.shutdown(Duration::from_secs(2)).await;
    assert!(!fixture.receipt().await.needs_attention);
    fixture.held(200, 2).await;
    fixture.assert_posts(2);
    assert_eq!(fixture.target.reads.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn late_mismatch_and_extra_root_never_recover_from_expected_pin() {
    for response in [
        format!("{{\"Hash\":\"{OTHER}\"}}\n"),
        format!("{{\"Hash\":\"{CID}\"}}\n{{\"Hash\":\"{OTHER}\"}}\n"),
    ] {
        let fixture = Fixture::new((StatusCode::OK, response)).await;
        let target = fixture.publish("a", CID).await;
        let worker = fixture
            .coordinator
            .start(fixture.store.clone(), CancellationToken::new());
        held_start(&fixture).await;
        fixture.takeover_park().await;
        fixture.target.release.add_permits(1);
        wait_for(|| async { fixture.receipt().await.observed_at.is_some() }).await;
        worker.shutdown(Duration::from_secs(2)).await;
        assert_eq!(fixture.receipt().await.outcome, "cid_mismatch");
        assert!(fixture.receipt().await.needs_attention);
        assert_ne!(fixture.target_state(&target).await, "pinned");
        assert_eq!(fixture.submit().await.state, "running");
        assert!(fixture.submit().await.locked_until.is_none());
        assert_eq!(
            fixture.target.reads.load(Ordering::SeqCst),
            0,
            "expected pin exists but cannot erase extra roots"
        );
        fixture.held(100, 1).await;
        fixture.assert_posts(1);
    }
}

#[tokio::test]
async fn receipt_and_recovery_wakeup_roll_back_together_without_new_post_or_release() {
    let fixture = Fixture::new((StatusCode::OK, format!("{{\"Hash\":\"{CID}\"}}\n"))).await;
    let target = fixture.publish("a", CID).await;
    let worker = fixture
        .coordinator
        .start(fixture.store.clone(), CancellationToken::new());
    held_start(&fixture).await;
    fixture.takeover_park().await;
    fixture.store.db().execute_unprepared("CREATE TRIGGER reject_receipt_wake BEFORE UPDATE OF state ON pin_jobs
        WHEN OLD.state='running' AND OLD.locked_until IS NULL AND NEW.state='pending' AND NEW.submit_phase='recovering'
        BEGIN SELECT RAISE(ABORT,'injected recovery CAS failure'); END;").await.unwrap();
    fixture.target.release.add_permits(1);
    wait_for(|| async { fixture.target.reads.load(Ordering::SeqCst) == 1 }).await;
    worker.shutdown(Duration::from_secs(2)).await;
    let receipt = fixture.receipt().await;
    assert_eq!(receipt.outcome, "in_flight");
    assert!(receipt.observed_at.is_none());
    assert_eq!(
        receipt.resources, "[]",
        "failed wakeup must roll back the receipt transaction too"
    );
    assert!(receipt.needs_attention);
    assert_eq!(fixture.submit().await.state, "running");
    assert!(fixture.submit().await.locked_until.is_none());
    assert_ne!(fixture.target_state(&target).await, "pinned");
    fixture
        .store
        .db()
        .execute_unprepared("DROP TRIGGER reject_receipt_wake")
        .await
        .unwrap();
    assert!(
        ledger::submission::has_debt(fixture.store.db(), &fixture.provider)
            .await
            .unwrap()
    );
    fixture.held(100, 1).await;
    fixture.assert_posts(1);
}

#[tokio::test]
#[ignore = "opt-in: requires IPFS_S3_TEST_RPC_RECOVERY_POSTGRES_URL to a NEW authorized isolated loopback PostgreSQL node"]
async fn postgres_actual_worker_receipt_lock_order_and_recoverable_waiters() {
    let url = std::env::var("IPFS_S3_TEST_RPC_RECOVERY_POSTGRES_URL").expect("explicit PostgreSQL selection requires a new authorized endpoint; absent URL is not a pass");
    let parsed = url::Url::parse(&url).unwrap();
    assert!(matches!(parsed.scheme(), "postgres" | "postgresql"));
    assert!(
        matches!(parsed.host_str(), Some("127.0.0.1" | "localhost" | "::1")),
        "only the parent-authorized isolated loopback test node is allowed"
    );
    assert_ne!(
        parsed.port(),
        Some(55467),
        "the old cleaned node must never be contacted"
    );
    let admin = store::connect_database(&url).await.unwrap();
    let schema = format!("rpc_recovery_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .unwrap();
    let mut options = ConnectOptions::new(url);
    options.set_schema_search_path(schema.clone());
    let db = match Database::connect(options).await {
        Ok(db) => db,
        Err(error) => {
            drop_owned_recovery_schema(&admin, &schema).await;
            admin.close().await.unwrap();
            panic!("isolated recovery schema connection failed: {error}");
        }
    };
    let close_db = db.clone();
    let cancellation = CancellationToken::new();
    let result = std::panic::AssertUnwindSafe(async {
        let fixture =
            Fixture::with_database((StatusCode::OK, format!("{{\"Hash\":\"{CID}\"}}\n")), db).await;
        let token = chrono::Timelike::with_nanosecond(&Utc::now(), 123_456_789).unwrap();
        let native = fixture
            .store
            .db()
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "SELECT $1::timestamptz AS token",
                [token.into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<chrono::DateTime<Utc>>("", "token")
            .unwrap();
        let sql_same = fixture
            .store
            .db()
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "SELECT $1::timestamptz = $2::timestamptz AS same",
                [token.into(), native.into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<bool>("", "same")
            .unwrap();
        assert_ne!(
            token, native,
            "PG persists the nanosecond caller token at native microsecond precision"
        );
        assert_eq!(chrono::Timelike::nanosecond(&native), 123_456_000);
        assert!(
            sql_same,
            "the native representation must still match the exact SQL claim fence"
        );
        let a = fixture.publish("a", CID).await;
        let worker = fixture
            .coordinator
            .start(fixture.store.clone(), cancellation.clone());
        held_start(&fixture).await;
        assert_eq!(
            ledger::submission::admission_barrier(fixture.store.db(), &fixture.provider)
                .await
                .unwrap(),
            ledger::submission::AdmissionBarrier::Temporary
        );
        let b = fixture.publish("b", OTHER).await;
        assert_eq!(fixture.target_state(&b).await, "quota_waiting");
        fixture.takeover_park().await;
        assert_eq!(
            ledger::submission::admission_barrier(fixture.store.db(), &fixture.provider)
                .await
                .unwrap(),
            ledger::submission::AdmissionBarrier::Operator
        );
        // The periodic scanner sees parked unknown debt until the complete
        // receipt arrives; B must remain a waiter, never permanently blocked.
        let txn = fixture.store.db().begin().await.unwrap();
        assert!(
            quota::wake_provider_waiters(
                &txn,
                &fixture.provider,
                &fixture.coordinator.provider_limits()[&fixture.provider],
                Utc::now()
            )
            .await
            .unwrap()
            .is_empty()
        );
        txn.commit().await.unwrap();
        assert_eq!(fixture.target_state(&b).await, "quota_waiting");
        fixture.target.release.add_permits(1);
        wait_for_stage("PG both targets pinned", || async {
            fixture.target_state(&a).await == "pinned" && fixture.target_state(&b).await == "pinned"
        })
        .await;
        worker.shutdown(Duration::from_secs(2)).await;
        fixture.held(200, 2).await;
        fixture.assert_posts(2);
        assert_eq!(fixture.target.reads.load(Ordering::SeqCst), 2);
        assert!(!fixture.receipt().await.needs_attention);
        assert!(
            !ledger::submission::has_debt(fixture.store.db(), &fixture.provider)
                .await
                .unwrap()
        );
    })
    .catch_unwind()
    .await;
    cancellation.cancel();
    close_db.close().await.unwrap();
    drop_owned_recovery_schema(&admin, &schema).await;
    admin.close().await.unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

async fn drop_owned_recovery_schema(admin: &DatabaseConnection, schema: &str) {
    let id = schema
        .strip_prefix("rpc_recovery_")
        .expect("unexpected schema prefix");
    assert!(
        id.len() == 32 && id.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "cleanup must be an exact UUID-owned schema, never public or another task"
    );
    admin
        .execute_unprepared(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .await
        .unwrap();
    let remaining = admin
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT COUNT(*) AS count FROM pg_namespace WHERE nspname=$1",
            [schema.to_owned().into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "count")
        .unwrap();
    assert_eq!(remaining, 0);
    eprintln!("isolated recovery schema cleanup verified: {schema}; remaining={remaining}");
}
