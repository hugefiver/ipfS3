use super::super::{publish_versioned_read_object, versioned_read_state};
use crate::config::{DownloadRedirectConfig, RateLimits};
use crate::import::{ImportConfig, downloader::SourceDownloader, pipeline::ImportCoordinator};
use crate::rate_limit::RateLimiter;
use crate::s3::handler::S3Impl;
use crate::s3::http::with_download_redirect;
use crate::s3::route::gateway::GatewayRoute;
use crate::s3::sigv4::{presign_sigv4_query, send_sigv4};
use crate::state::AppState;
use axum::error_handling::HandleError;
use base64::Engine as _;
use http::{HeaderMap, Method, StatusCode};
use std::{net::SocketAddr, sync::Arc};

pub(super) async fn serve_object_http(
    state: &Arc<AppState>,
    download_redirect: Option<DownloadRedirectConfig>,
) -> (String, tokio::task::JoinHandle<()>) {
    serve_object_http_with_rate_limits(state, download_redirect, RateLimits::default()).await
}

pub(super) async fn serve_object_http_with_rate_limits(
    state: &Arc<AppState>,
    download_redirect: Option<DownloadRedirectConfig>,
    rate_limits: RateLimits,
) -> (String, tokio::task::JoinHandle<()>) {
    let limiter = RateLimiter::new(rate_limits);
    let mut builder = s3s::service::S3ServiceBuilder::new(S3Impl::new(state.clone()));
    builder.set_auth(crate::auth::GatewayAuth::new(state.clone()));
    builder.set_access(crate::access::GatewayAccess(state.clone()));
    if limiter.is_some() {
        let config = ImportConfig::default().validate().unwrap();
        let downloader = SourceDownloader::production(Arc::new(config.clone()));
        let imports = ImportCoordinator::new(config, downloader);
        builder.set_route(GatewayRoute::new(state.clone(), imports));
    }
    let app = axum::Router::new().fallback_service(HandleError::new(
        builder.build(),
        |_: s3s::HttpError| async { StatusCode::INTERNAL_SERVER_ERROR },
    ));
    let app = with_download_redirect(app, download_redirect);
    let app = if let Some(limiter) = limiter {
        app.layer(axum::Extension(Arc::new(limiter)))
    } else {
        app
    };
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", socket.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(
            socket,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap()
    });
    (endpoint, server)
}

#[tokio::test]
async fn signed_copy_batch_delete_and_presigned_lists_obey_literal_object_scope() {
    let (mut state, _kubo) = versioned_read_state().await;
    let mut config = crate::config::Config::default_for_test();
    config.auth = toml::from_str(r#"
        [[credentials]]
        access_key = 'test'
        secret_key = 'test'
        [[policy]]
        effect = 'allow'
        user = 'test'
        bucket = 'bucket'
        prefix = 'Team_%/'
        actions = ['ListObjects', 'ListObjectsV2', 'ListObjectVersions', 'CopyObject', 'GetObject', 'DeleteObject']
    "#).unwrap();
    config.auth.session_signing_key = Some("0f".repeat(32));
    Arc::get_mut(&mut state).unwrap().credentials =
        crate::auth::GatewayCredentials::from_config(&config).unwrap();
    let session = state.credentials.issue_session("test", 300).unwrap();
    for (id, key) in [
        ("one", "Team_%/owned"),
        ("two", "team_%/private"),
        ("three", "TeamXA/elsewhere"),
    ] {
        let tags = if id == "one" {
            vec![crate::pinning::tags::ObjectTag::new("owner", "team")]
        } else {
            vec![]
        };
        publish_versioned_read_object(&state, id, key, "QmExample", false, None, None, tags).await;
    }
    let (endpoint, server) = serve_object_http(&state, None).await;

    for mut query in [vec![], vec![("list-type", "2")], vec![("versions", "")]] {
        query.push(("prefix", "Team_%/"));
        let url = presign_sigv4_query(
            &Method::GET,
            &endpoint,
            "bucket",
            "",
            &query,
            &session.access_key_id,
            &session.secret_access_key,
            Some(&session.session_token),
            60,
            chrono::Utc::now(),
        );
        let response = reqwest::get(url).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let xml = response.text().await.unwrap();
        assert!(xml.contains("<Key>Team_%/owned</Key>"), "{xml}");
        assert!(
            !xml.contains("/private") && !xml.contains("/elsewhere"),
            "{xml}"
        );
    }
    let wider = send_sigv4(
        Method::GET,
        &endpoint,
        "bucket",
        "",
        &[("list-type", "2"), ("prefix", "Team")],
        vec![],
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(wider.status(), StatusCode::FORBIDDEN);

    for (source, destination) in [
        ("team_%25/private", "Team_%/copy"),
        ("Team_%25/owned", "other/copy"),
    ] {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-copy-source",
            format!("/bucket/{source}").parse().unwrap(),
        );
        let copy = send_sigv4(
            Method::PUT,
            &endpoint,
            "bucket",
            destination,
            &[],
            vec![],
            headers,
            "test",
        )
        .await;
        let status = copy.status();
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "{}",
            copy.text().await.unwrap()
        );
    }

    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-copy-source",
        "/bucket/Team_%25/owned".parse().unwrap(),
    );
    let copy = send_sigv4(
        Method::PUT,
        &endpoint,
        "bucket",
        "Team_%/copy",
        &[],
        vec![],
        headers,
        "test",
    )
    .await;
    assert_eq!(
        copy.status(),
        StatusCode::OK,
        "{}",
        copy.text().await.unwrap()
    );
    let db = state.store.db();
    let copied = crate::store::object::get_latest(db, "bucket", "Team_%/copy")
        .await
        .unwrap();
    assert_eq!(
        crate::store::pinning::tags::list_object_tags(db, &copied.id)
            .await
            .unwrap(),
        vec![crate::pinning::tags::ObjectTag::new("owner", "team")]
    );

    let body = b"<Delete><Object><Key>team_%/private</Key></Object><Object><Key>Team_%/owned</Key></Object></Delete>";
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/xml".parse().unwrap());
    headers.insert(
        "content-md5",
        base64::engine::general_purpose::STANDARD
            .encode(md5::compute(body).0)
            .parse()
            .unwrap(),
    );
    let batch = send_sigv4(
        Method::POST,
        &endpoint,
        "bucket",
        "",
        &[("delete", "")],
        body.to_vec(),
        headers,
        "test",
    )
    .await;
    assert_eq!(batch.status(), StatusCode::OK);
    let xml = batch.text().await.unwrap();
    assert!(
        xml.contains("<Code>AccessDenied</Code>") && xml.contains("<Deleted>"),
        "{xml}"
    );
    let db = state.store.db();
    assert!(
        crate::store::object::get_latest(db, "bucket", "Team_%/owned")
            .await
            .is_err()
    );
    assert!(
        crate::store::object::get_latest(db, "bucket", "team_%/private")
            .await
            .is_ok()
    );
    server.abort();
}
