//! Production-equivalent gateway surface for lifecycle-transition process evidence.

use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::{Router, error_handling::HandleError, middleware};
use http::{HeaderMap, StatusCode, header};
use ipfs_s3_gateway::{
    auth::GatewayAuth,
    import::{ImportConfig, downloader::SourceDownloader, pipeline::ImportCoordinator},
    kubo::KuboClient,
    s3::{handler::S3Impl, route::gateway::GatewayRoute},
    state::AppState,
    store::Store,
};
use s3s::{Body as S3Body, HttpError, service::S3ServiceBuilder, validation::AwsNameValidation};
use sea_orm::DatabaseConnection;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use super::{SeededTransition, sigv4};

pub(super) struct ProcessGateway {
    endpoint: String,
    shutdown: CancellationToken,
    join: tokio::task::JoinHandle<()>,
}

impl ProcessGateway {
    pub(super) async fn start(db: DatabaseConnection, hot: KuboClient, cold: KuboClient) -> Self {
        let state = Arc::new(AppState {
            kubo: hot,
            cold_kubo: Some(cold),
            store: Store::new(db),
            credentials: HashMap::from([("test".to_owned(), s3s::auth::SecretKey::from("test"))]),
            master_key: ipfs_s3_gateway::crypto::MasterKey::from_hex(&"0".repeat(64))
                .expect("process gateway test master key"),
            pinning: ipfs_s3_gateway::pinning::coordinator::PinningCoordinator::disabled_for_test(),
        });
        let import_config = ImportConfig::default()
            .validate()
            .expect("validate process gateway import configuration");
        let downloader = SourceDownloader::production(Arc::new(import_config.clone()));
        let imports = ImportCoordinator::new(import_config, downloader);
        let mut builder = S3ServiceBuilder::new(S3Impl::new(state.clone()));
        builder.set_validation(AwsNameValidation::new());
        builder.set_auth(GatewayAuth::new(state.clone()));
        builder.set_route(GatewayRoute::new(state.clone(), imports));
        let app = Router::new()
            .fallback_service(HandleError::new(builder.build(), process_gateway_error))
            .layer(middleware::from_fn(
                ipfs_s3_gateway::s3::http::bridge_chunked_content_length,
            ))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                ipfs_s3_gateway::cors::http::bucket_cors,
            ))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind process gateway listener");
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let join = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(server_shutdown.cancelled_owned())
                .await
                .expect("serve process gateway");
        });
        Self {
            endpoint,
            shutdown,
            join,
        }
    }

    pub(super) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub(super) async fn shutdown(mut self) {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), &mut self.join)
            .await
            .expect("process gateway shutdown timed out")
            .expect("process gateway task panicked");
    }
}

impl Drop for ProcessGateway {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.join.abort();
    }
}

pub(super) async fn assert_gateway_object(
    endpoint: &str,
    object: &SeededTransition,
    expected_storage_class: &str,
    local_kubo: &KuboClient,
    checkpoint: &str,
) {
    let get = signed_object_request(endpoint, reqwest::Method::GET, object).await;
    assert_eq!(get.status(), StatusCode::OK, "{checkpoint} GET status");
    assert_object_response_headers(&get, object, expected_storage_class, checkpoint, "GET");
    let get_body = get.bytes().await.expect("read process gateway GET body");
    assert_eq!(
        get_body.as_ref(),
        object.body.as_slice(),
        "{checkpoint} GET bytes"
    );

    let head = signed_object_request(endpoint, reqwest::Method::HEAD, object).await;
    assert_eq!(head.status(), StatusCode::OK, "{checkpoint} HEAD status");
    assert_object_response_headers(&head, object, expected_storage_class, checkpoint, "HEAD");

    let local = local_kubo
        .verify_local_residency(&object.cid)
        .await
        .unwrap_or_else(|error| panic!("{checkpoint} local CID verification failed: {error}"));
    assert_eq!(local.cid, object.cid, "{checkpoint} local CID");
    let local_bytes = ipfs_s3_gateway::kubo::cat::cat_to_vec(local_kubo, &object.cid)
        .await
        .unwrap_or_else(|error| panic!("{checkpoint} local Kubo cat failed: {error}"));
    assert_eq!(local_bytes, object.body, "{checkpoint} local Kubo bytes");
    eprintln!(
        "transition-process evidence event=gateway-read checkpoint={checkpoint} method=get+head storage_class={expected_storage_class} cid={} bytes={} version_id={}",
        object.cid,
        object.body.len(),
        object.public_version_id
    );
}

pub(super) async fn delete_gateway_object(endpoint: &str, object: &SeededTransition) {
    let response = signed_object_request(endpoint, reqwest::Method::DELETE, object).await;
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "DELETE in-flight transition target"
    );
}

pub(super) async fn assert_gateway_no_such_key(endpoint: &str, object: &SeededTransition) {
    let get = signed_object_request(endpoint, reqwest::Method::GET, object).await;
    assert_eq!(get.status(), StatusCode::NOT_FOUND, "deleted target GET");
    let body = get.text().await.expect("read deleted target GET error");
    assert!(
        body.contains("<Code>NoSuchKey</Code>"),
        "deleted target GET must return NoSuchKey: {body}"
    );

    let head = signed_object_request(endpoint, reqwest::Method::HEAD, object).await;
    assert_eq!(head.status(), StatusCode::NOT_FOUND, "deleted target HEAD");
}

async fn signed_object_request(
    endpoint: &str,
    method: reqwest::Method,
    object: &SeededTransition,
) -> reqwest::Response {
    tokio::time::timeout(
        Duration::from_secs(20),
        sigv4::send_sigv4(
            method,
            endpoint,
            &object.bucket,
            &object.key,
            &[],
            Vec::new(),
            HeaderMap::new(),
            "test",
        ),
    )
    .await
    .expect("signed process gateway request timed out")
}

fn assert_object_response_headers(
    response: &reqwest::Response,
    object: &SeededTransition,
    expected_storage_class: &str,
    checkpoint: &str,
    method: &str,
) {
    let expected_length = object.body.len().to_string();
    let expected_etag = format!("\"{}\"", object.cid);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some(expected_length.as_str()),
        "{checkpoint} {method} Content-Length"
    );
    assert_eq!(
        response
            .headers()
            .get(header::ETAG)
            .and_then(|value| value.to_str().ok()),
        Some(expected_etag.as_str()),
        "{checkpoint} {method} ETag/CID"
    );
    assert_eq!(
        response
            .headers()
            .get("x-amz-storage-class")
            .and_then(|value| value.to_str().ok()),
        Some(expected_storage_class),
        "{checkpoint} {method} storage class"
    );
    assert_eq!(
        response
            .headers()
            .get("x-amz-version-id")
            .and_then(|value| value.to_str().ok()),
        Some(object.public_version_id.as_str()),
        "{checkpoint} {method} public version"
    );
}

async fn process_gateway_error(_error: HttpError) -> http::Response<S3Body> {
    http::Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(S3Body::from("Internal Server Error".to_owned()))
        .expect("build process gateway error response")
}
