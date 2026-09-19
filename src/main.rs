use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::Router;
use axum::error_handling::HandleError;
use axum::extract::State;
use axum::http::{Response as HttpResponse, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use ipfs_s3_gateway::auth::GatewayAuth;
use ipfs_s3_gateway::config::Config;
use ipfs_s3_gateway::import::{downloader::SourceDownloader, pipeline::ImportCoordinator};
use ipfs_s3_gateway::lifecycle::worker::start_worker_with_tiers;
use ipfs_s3_gateway::residency::backfill::start_worker as start_residency_backfill_worker;
use ipfs_s3_gateway::s3;
use ipfs_s3_gateway::s3::handler::S3Impl;
use ipfs_s3_gateway::state::AppState;
use s3s::service::S3ServiceBuilder;
use s3s::validation::AwsNameValidation;
use s3s::{Body as S3Body, HttpError};
use sea_orm::DbErr;

mod shutdown;

const READY_DEADLINE: Duration = Duration::from_secs(2);
const READY_PROBE_URL: &str = "http://127.0.0.1:9000/ready";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunMode {
    Gateway,
    ReadyProbe,
}

async fn health_check() -> &'static str {
    "OK"
}

fn parse_run_mode(args: &[String]) -> anyhow::Result<RunMode> {
    match args {
        [] => Ok(RunMode::Gateway),
        [flag] if flag == "--ready-probe" => Ok(RunMode::ReadyProbe),
        _ => anyhow::bail!("usage: ipfs-s3-gateway [--ready-probe]"),
    }
}

async fn readiness_response<F>(ping: F, deadline: Duration) -> Response
where
    F: Future<Output = Result<(), DbErr>>,
{
    match tokio::time::timeout(deadline, ping).await {
        Ok(Ok(())) => (StatusCode::OK, "READY").into_response(),
        Ok(Err(_)) => {
            tracing::warn!(failure = "error");
            (StatusCode::SERVICE_UNAVAILABLE, "NOT READY").into_response()
        }
        Err(_) => {
            tracing::warn!(failure = "timeout");
            (StatusCode::SERVICE_UNAVAILABLE, "NOT READY").into_response()
        }
    }
}

async fn ready_handler(State(state): State<Arc<AppState>>) -> Response {
    readiness_response(state.store.db().ping(), READY_DEADLINE).await
}

async fn ready_probe_url(url: &str, deadline: Duration) -> bool {
    let started = Instant::now();
    let client = match reqwest::Client::builder().build() {
        Ok(client) => client,
        Err(_) => return false,
    };
    matches!(
        tokio::time::timeout(deadline.saturating_sub(started.elapsed()), async {
            let response = client.get(url).send().await.ok()?;
            if response.status().as_u16() != 200 {
                return Some(false);
            }
            Some(response.text().await.ok()? == "READY")
        })
        .await,
        Ok(Some(true))
    )
}

async fn ready_probe() -> bool {
    ready_probe_url(READY_PROBE_URL, READY_DEADLINE).await
}

async fn handle_s3_error(err: HttpError) -> HttpResponse<S3Body> {
    tracing::error!(?err, "s3 service error");
    HttpResponse::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(S3Body::from("Internal Server Error".to_string()))
        .unwrap()
}

fn gateway_app(state: Arc<AppState>, imports: Arc<ImportCoordinator>) -> Router {
    let s3_impl = S3Impl::new(state.clone());
    let gateway_auth = GatewayAuth::new(state.clone());

    let s3_service = {
        let mut builder = S3ServiceBuilder::new(s3_impl);
        builder.set_validation(AwsNameValidation::new());
        builder.set_auth(gateway_auth);
        builder.set_route(s3::route::gateway::GatewayRoute::new(
            state.clone(),
            imports,
        ));
        builder.build()
    };

    Router::new()
        .route("/health", get(health_check))
        .route("/ready", get(ready_handler))
        .fallback_service(HandleError::new(s3_service, handle_s3_error))
        .layer(axum::middleware::from_fn(
            s3::http::bridge_chunked_content_length,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            ipfs_s3_gateway::cors::http::bucket_cors,
        ))
        .with_state(state)
}

async fn run_gateway() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let cfg = Config::load()?;
    let lifecycle_config = cfg.lifecycle.validate()?;
    tracing::info!(bind = %cfg.server.bind, kubo = %cfg.kubo.rpc_url, "starting ipfs-s3-gateway");

    let state = AppState::new(&cfg).await?;
    let import_config = cfg.imports.validate()?;
    let downloader = SourceDownloader::production(Arc::new(import_config.clone()));
    let imports = ImportCoordinator::new(import_config, downloader);

    let app = gateway_app(state.clone(), imports.clone());

    let signal = shutdown::ShutdownSignal::install()?;
    let listener = tokio::net::TcpListener::bind(cfg.server.bind).await?;
    tracing::info!("listening on {}", cfg.server.bind);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let pinning_worker = state
        .pinning
        .start(state.store.clone(), shutdown.child_token());
    let import_worker = imports.start(state.clone(), shutdown.child_token());
    let lifecycle_worker = start_worker_with_tiers(
        state.store.clone(),
        lifecycle_config,
        shutdown.child_token(),
        state.kubo.clone(),
        state.cold_kubo.clone(),
    );
    let residency_backfill_worker = start_residency_backfill_worker(
        state.store.clone(),
        state.kubo.clone(),
        shutdown.child_token(),
    );
    let server =
        axum::serve(listener, app).with_graceful_shutdown(shutdown.clone().cancelled_owned());
    let workers = async move {
        tokio::join!(
            pinning_worker.shutdown(shutdown::WORKER_GRACE),
            import_worker.shutdown(shutdown::WORKER_GRACE),
            lifecycle_worker.shutdown(shutdown::WORKER_GRACE),
            residency_backfill_worker.shutdown(shutdown::WORKER_GRACE)
        );
    };
    let result = shutdown::drain(
        async move { server.await },
        signal.wait(),
        shutdown,
        workers,
        shutdown::EXIT_BUDGET,
    )
    .await;
    if let Err(error) = &result
        && error.kind() == std::io::ErrorKind::TimedOut
    {
        // Dropping axum's serve future alone does not terminate its spawned
        // connection tasks. End the process so no late writer/renewer survives.
        // Unfinished ownership cleanup is recovered by fenced DB lease expiry.
        tracing::error!(%error, "forcing process exit; unfinished leases recover by expiry");
        std::process::exit(1);
    }
    result?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match parse_run_mode(&args)? {
        RunMode::Gateway => run_gateway().await,
        RunMode::ReadyProbe => {
            if ready_probe().await {
                Ok(())
            } else {
                std::process::exit(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        io::{self, Write},
        sync::{Arc, Mutex},
        time::Duration,
    };

    use axum::{
        Router,
        body::{Body, to_bytes},
        http::{Request, StatusCode},
        routing::get,
    };
    use ipfs_s3_gateway::{
        crypto::key::MasterKey, kubo::KuboClient, pinning::coordinator::PinningCoordinator,
        store::Store,
    };
    use sea_orm::{ColumnTrait, Database, EntityTrait, QueryFilter};
    use tokio::{net::TcpListener, task::JoinHandle};
    use tower::ServiceExt as _;

    use super::*;

    #[derive(Clone)]
    struct CapturedWriter(Arc<Mutex<Vec<u8>>>);

    struct LogWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for LogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.0.lock().unwrap().flush()
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedWriter {
        type Writer = LogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            LogWriter(self.0.clone())
        }
    }

    async fn test_state() -> Arc<AppState> {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        ipfs_s3_gateway::store::run_migrations(&db).await.unwrap();
        Arc::new(AppState {
            kubo: KuboClient::new("http://127.0.0.1:1".to_owned()),
            cold_kubo: None,
            store: Store::new(db),
            credentials: HashMap::new(),
            master_key: MasterKey::from_hex(&"0".repeat(64)).unwrap(),
            pinning: PinningCoordinator::disabled_for_test(),
        })
    }

    async fn response_body(response: axum::response::Response) -> String {
        String::from_utf8(
            to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap()
    }

    fn captured_warn_subscriber() -> (Arc<Mutex<Vec<u8>>>, tracing::Dispatch) {
        let output = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .without_time()
            .with_ansi(false)
            .with_writer(CapturedWriter(output.clone()))
            .finish();
        (output, tracing::Dispatch::new(subscriber))
    }

    async fn start_probe_server(
        status: StatusCode,
        body: &'static str,
        delay: Duration,
    ) -> (String, JoinHandle<()>) {
        let app = Router::new().route(
            "/ready",
            get(move || async move {
                tokio::time::sleep(delay).await;
                (status, body)
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (url, server)
    }

    #[tokio::test]
    async fn health_check_returns_ok() {
        assert_eq!(health_check().await, "OK");
    }

    #[tokio::test]
    async fn lifecycle_http_request_does_not_start_a_worker() {
        let state = test_state().await;
        ipfs_s3_gateway::store::bucket::create(state.store.db(), "bucket", None)
            .await
            .unwrap();
        let now = ipfs_s3_gateway::store::database_clock::database_now(state.store.db())
            .await
            .unwrap();
        let mut action = ipfs_s3_gateway::lifecycle::model::NewLifecycleAction {
            idempotency_key: String::new(),
            bucket: "bucket".to_owned(),
            config_revision: 1,
            rule_identity: ipfs_s3_gateway::lifecycle::model::RuleIdentity::Id("expire".to_owned()),
            action_kind: ipfs_s3_gateway::lifecycle::model::LifecycleActionKind::ExpireCurrent,
            target: ipfs_s3_gateway::lifecycle::model::LifecycleTargetIdentity::Version(
                ipfs_s3_gateway::lifecycle::model::VersionTargetIdentity {
                    bucket: "bucket".to_owned(),
                    key: "object".to_owned(),
                    version_row_id: "missing-version".to_owned(),
                    public_version_id:
                        ipfs_s3_gateway::store::object_version::PublicVersionId::Null,
                    kind: ipfs_s3_gateway::store::object_version::VersionKind::Object,
                    object_id: Some("missing-object".to_owned()),
                    sequence: 1,
                },
            ),
            due_at: now,
        };
        action.idempotency_key =
            ipfs_s3_gateway::store::lifecycle_action::idempotency_key(&action).unwrap();
        let action_key = action.idempotency_key.clone();
        assert!(
            ipfs_s3_gateway::store::lifecycle_action::insert_idempotent(
                state.store.db(),
                action,
                now,
            )
            .await
            .unwrap()
        );
        let import_config = ipfs_s3_gateway::import::ImportConfig::default()
            .validate()
            .unwrap();
        let imports = ImportCoordinator::new(
            import_config.clone(),
            SourceDownloader::production(Arc::new(import_config)),
        );
        let app = gateway_app(state.clone(), imports);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let action = ipfs_s3_gateway::store::entities::lifecycle_action::Entity::find()
            .filter(
                ipfs_s3_gateway::store::entities::lifecycle_action::Column::IdempotencyKey
                    .eq(action_key),
            )
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(action.state, "pending");
    }

    #[tokio::test]
    async fn ready_handler_returns_ready_when_database_is_available() {
        let app = Router::new()
            .route("/ready", get(ready_handler))
            .with_state(test_state().await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response_body(response).await, "READY");
    }

    #[tokio::test]
    async fn readiness_database_error_is_redacted_and_classified() {
        let (output, subscriber) = captured_warn_subscriber();
        let response = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            tracing::dispatcher::with_default(&subscriber, || {
                runtime.block_on(readiness_response(
                    async {
                        Err(sea_orm::DbErr::Custom(
                            "postgres://user:password@db/internal".to_owned(),
                        ))
                    },
                    Duration::from_millis(10),
                ))
            })
        })
        .join()
        .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response_body(response).await, "NOT READY");
        let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert!(logs.contains("failure=\"error\""), "logs: {logs}");
        assert!(!logs.contains("postgres://user:password@db/internal"));
    }

    #[tokio::test]
    async fn readiness_timeout_is_redacted_and_classified() {
        let (output, subscriber) = captured_warn_subscriber();
        let response = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            tracing::dispatcher::with_default(&subscriber, || {
                runtime.block_on(readiness_response(
                    std::future::pending::<Result<(), sea_orm::DbErr>>(),
                    Duration::from_millis(10),
                ))
            })
        })
        .join()
        .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response_body(response).await, "NOT READY");
        let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert!(logs.contains("failure=\"timeout\""), "logs: {logs}");
        assert!(!logs.contains("http://127.0.0.1:9000/ready"));
    }

    #[test]
    fn parse_run_mode_accepts_only_the_supported_argument_shapes() {
        assert_eq!(parse_run_mode(&[]).unwrap(), RunMode::Gateway);
        assert_eq!(
            parse_run_mode(&["--ready-probe".to_owned()]).unwrap(),
            RunMode::ReadyProbe
        );
        assert!(parse_run_mode(&["--unknown".to_owned()]).is_err());
        assert!(parse_run_mode(&["--ready-probe".to_owned(), "extra".to_owned()]).is_err());
    }

    #[tokio::test]
    async fn ready_probe_requires_an_exact_ready_response_within_deadline() {
        let (url, server) = start_probe_server(StatusCode::OK, "READY", Duration::ZERO).await;
        assert!(ready_probe_url(&format!("{url}/ready"), Duration::from_millis(100)).await);
        server.abort();

        let (url, server) =
            start_probe_server(StatusCode::SERVICE_UNAVAILABLE, "READY", Duration::ZERO).await;
        assert!(!ready_probe_url(&format!("{url}/ready"), Duration::from_millis(100)).await);
        server.abort();

        let (url, server) = start_probe_server(StatusCode::OK, "READY\n", Duration::ZERO).await;
        assert!(!ready_probe_url(&format!("{url}/ready"), Duration::from_millis(100)).await);
        server.abort();

        let (url, server) =
            start_probe_server(StatusCode::OK, "READY", Duration::from_secs(1)).await;
        assert!(!ready_probe_url(&format!("{url}/ready"), Duration::from_millis(10)).await);
        server.abort();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/ready", listener.local_addr().unwrap());
        drop(listener);
        assert!(!ready_probe_url(&url, Duration::from_millis(100)).await);
    }
}
