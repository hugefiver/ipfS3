//! Production-equivalent gateway wiring for focused HTTP integration tests.

use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::{
    Router,
    error_handling::HandleError,
    extract::State,
    http::{Response as HttpResponse, StatusCode},
    middleware,
    response::{IntoResponse, Response},
    routing::get,
};
use ipfs_s3_gateway::{
    auth::GatewayAuth,
    import::{ImportConfig, downloader::SourceDownloader, pipeline::ImportCoordinator},
    s3::{handler::S3Impl, route::gateway::GatewayRoute},
    state::AppState,
    store::{self, Store},
};
use s3s::{Body as S3Body, HttpError, service::S3ServiceBuilder, validation::AwsNameValidation};
use sea_orm::Database;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers};

const READY_DEADLINE: Duration = Duration::from_secs(2);

pub struct S3ServerHandle {
    pub endpoint: String,
    cancellation: tokio_util::sync::CancellationToken,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl S3ServerHandle {
    pub async fn shutdown(mut self) {
        self.cancellation.cancel();
        if let Some(mut join) = self.join.take() {
            match tokio::time::timeout(Duration::from_secs(2), &mut join).await {
                Ok(result) => result.expect("test S3 server task failed"),
                Err(_) => {
                    join.abort();
                    let _ = join.await;
                }
            }
        }
    }
}

impl Drop for S3ServerHandle {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }
}

pub struct CorsHarness {
    pub endpoint: String,
    pub bucket: String,
    pub state: Arc<AppState>,
    pub kubo: MockServer,
    pub imports: Arc<ImportCoordinator>,
    _server: S3ServerHandle,
}

impl CorsHarness {
    pub async fn mount_cat(&self, cid: &str, body: &[u8]) {
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/api/v0/cat"))
            .and(matchers::query_param("arg", cid))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
            .mount(&self.kubo)
            .await;
    }

    pub async fn mount_add_failure(&self) {
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/api/v0/add"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&self.kubo)
            .await;
    }

    pub async fn kubo_request_count(&self) -> usize {
        self.kubo
            .received_requests()
            .await
            .expect("Kubo request log")
            .len()
    }

    pub async fn seed_plain_object(&self, key: &str, cid: &str, body: &[u8]) {
        self.mount_cat(cid, body).await;
        self.publish_plain_object(key, cid, body.len()).await;
    }

    pub async fn publish_plain_object(&self, key: &str, cid: &str, size: usize) {
        use ipfs_s3_gateway::{
            pinning::policy::PublicationPolicy,
            store::pinning::publication::{
                PinTargetSpec, PublicationObject, PublicationRequest, publish_object,
            },
        };

        let object = PublicationObject::from_put(
            uuid::Uuid::new_v4().to_string(),
            &self.bucket,
            key,
            cid.to_owned(),
            size as i64,
            Some("application/octet-stream".to_owned()),
            None,
            false,
            None,
            None,
            chrono::Utc::now(),
        );
        publish_object(
            self.state.store.db(),
            PublicationRequest {
                object: object.clone(),
                tags: Vec::new(),
                policy: PublicationPolicy {
                    tags: Vec::new(),
                    leases: Vec::new(),
                },
                object_target: PinTargetSpec {
                    cid: object.cid,
                    logical_size: object.logical_size,
                },
            },
            self.state.pinning.provider_limits(),
        )
        .await
        .expect("publish focused CORS test object through the central publication path");
    }
}

pub fn default_import_coordinator() -> Arc<ImportCoordinator> {
    let config = ImportConfig::default()
        .validate()
        .expect("validate default import configuration");
    let downloader = SourceDownloader::production(Arc::new(config.clone()));
    ImportCoordinator::new(config, downloader)
}

pub fn gateway_router(state: Arc<AppState>, imports: Arc<ImportCoordinator>) -> Router {
    let s3_impl = S3Impl::new(state.clone());
    let mut builder = S3ServiceBuilder::new(s3_impl);
    builder.set_validation(AwsNameValidation::new());
    builder.set_auth(GatewayAuth::new(state.clone()));
    builder.set_route(GatewayRoute::new(state.clone(), imports));

    Router::new()
        .route("/health", get(health_check))
        .route("/ready", get(ready_handler))
        .fallback_service(HandleError::new(builder.build(), handle_s3_error))
        .layer(middleware::from_fn(
            ipfs_s3_gateway::s3::http::bridge_chunked_content_length,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            ipfs_s3_gateway::cors::http::bucket_cors,
        ))
        .with_state(state)
}

pub async fn start_router(app: Router) -> S3ServerHandle {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test S3 listener");
    let port = listener.local_addr().expect("test listener address").port();
    let cancellation = tokio_util::sync::CancellationToken::new();
    let server_cancellation = cancellation.clone();
    let join = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(server_cancellation.cancelled_owned())
            .await
            .expect("test S3 server terminated unexpectedly");
    });

    S3ServerHandle {
        endpoint: format!("http://127.0.0.1:{port}"),
        cancellation,
        join: Some(join),
    }
}

pub async fn start_gateway(
    state: Arc<AppState>,
    imports: Arc<ImportCoordinator>,
) -> S3ServerHandle {
    start_router(gateway_router(state, imports)).await
}

pub async fn start_harness() -> CorsHarness {
    let kubo = MockServer::start().await;
    let db = Database::connect("sqlite::memory:")
        .await
        .expect("in-memory SQLite database");
    store::run_migrations(&db)
        .await
        .expect("run focused CORS test migrations");
    let bucket = "cors-bucket".to_owned();
    store::bucket::create(&db, &bucket, Some("owner"))
        .await
        .expect("create focused CORS test bucket");
    let state = Arc::new(AppState {
        kubo: ipfs_s3_gateway::kubo::KuboClient::new(kubo.uri()),
        store: Store::new(db),
        credentials: HashMap::from([("test".to_owned(), s3s::auth::SecretKey::from("test"))]),
        master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(&"0".repeat(64))
            .expect("zero test master key"),
        pinning: ipfs_s3_gateway::pinning::coordinator::PinningCoordinator::disabled_for_test(),
    });
    let imports = default_import_coordinator();
    let server = start_gateway(state.clone(), imports.clone()).await;

    CorsHarness {
        endpoint: server.endpoint.clone(),
        bucket,
        state,
        kubo,
        imports,
        _server: server,
    }
}

async fn health_check() -> &'static str {
    "OK"
}

async fn ready_handler(State(state): State<Arc<AppState>>) -> Response {
    match tokio::time::timeout(READY_DEADLINE, state.store.db().ping()).await {
        Ok(Ok(())) => (StatusCode::OK, "READY").into_response(),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "NOT READY").into_response(),
    }
}

async fn handle_s3_error(_error: HttpError) -> HttpResponse<S3Body> {
    HttpResponse::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(S3Body::from("Internal Server Error".to_owned()))
        .expect("internal error response")
}
