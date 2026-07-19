use axum::Router;
use axum::error_handling::HandleError;
use axum::http::{Response, StatusCode};
use axum::routing::get;
use s3s::service::S3ServiceBuilder;
use s3s::{Body as S3Body, HttpError};

mod auth;
mod config;
mod crypto;
mod error;
mod kubo;
mod pinning;
mod s3;
mod state;
mod store;
mod zip;

use auth::GatewayAuth;
use config::Config;
use s3::handler::S3Impl;
use state::AppState;

async fn health_check() -> &'static str {
    "OK"
}

async fn handle_s3_error(err: HttpError) -> Response<S3Body> {
    tracing::error!(?err, "s3 service error");
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(S3Body::from("Internal Server Error".to_string()))
        .unwrap()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let cfg = Config::load()?;
    tracing::info!(bind = %cfg.server.bind, kubo = %cfg.kubo.rpc_url, "starting ipfs-s3-gateway");

    let state = AppState::new(&cfg).await?;

    let s3_impl = S3Impl::new(state.clone());
    let gateway_auth = GatewayAuth::new(state.clone());

    let s3_service = {
        let mut builder = S3ServiceBuilder::new(s3_impl);
        builder.set_auth(gateway_auth);
        builder.set_route(crate::s3::route::decompress_zip::DecompressZipRoute::new(
            state.clone(),
        ));
        builder.build()
    };

    let s3_service = HandleError::new(s3_service, handle_s3_error);

    let app = Router::new()
        .route("/health", get(health_check))
        .fallback_service(s3_service)
        .layer(axum::middleware::from_fn(
            crate::s3::http::bridge_chunked_content_length,
        ));

    let listener = tokio::net::TcpListener::bind(cfg.server.bind).await?;
    tracing::info!("listening on {}", cfg.server.bind);
    axum::serve(listener, app).await?;

    Ok(())
}
