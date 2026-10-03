use std::sync::Arc;

use http::{HeaderMap, Method, Uri};
use s3s::route::S3Route;
use s3s::{Body, S3Request, S3Response, S3Result};

use crate::{
    import::pipeline::ImportCoordinator,
    s3::route::{decompress_zip::DecompressZipRoute, import_object::ImportObjectRoute},
    state::AppState,
};

pub struct GatewayRoute {
    imports: ImportObjectRoute,
    decompress: DecompressZipRoute,
}

impl GatewayRoute {
    pub fn new(state: Arc<AppState>, coordinator: Arc<ImportCoordinator>) -> Self {
        Self::with_root_default(state, coordinator, true)
    }

    pub fn with_root_default(
        state: Arc<AppState>,
        coordinator: Arc<ImportCoordinator>,
        root_default: bool,
    ) -> Self {
        Self {
            imports: ImportObjectRoute::with_root_default(state.clone(), coordinator, root_default),
            decompress: DecompressZipRoute::new(state),
        }
    }
}

#[async_trait::async_trait]
impl S3Route for GatewayRoute {
    fn is_match(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        extensions: &mut http::Extensions,
    ) -> bool {
        self.imports.is_match(method, uri, headers, extensions)
            || self.decompress.is_match(method, uri, headers, extensions)
    }

    async fn call(&self, mut req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        if self
            .imports
            .is_match(&req.method, &req.uri, &req.headers, &mut req.extensions)
        {
            return self.imports.call_authenticated(req).await;
        }
        if self
            .decompress
            .is_match(&req.method, &req.uri, &req.headers, &mut req.extensions)
        {
            return self.decompress.call_authenticated(req).await;
        }
        Err(s3s::s3_error!(
            MethodNotAllowed,
            "request does not match a custom gateway route"
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use axum::error_handling::HandleError;
    use http::StatusCode;
    use s3s::service::S3ServiceBuilder;
    use sea_orm::{Database, EntityTrait, PaginatorTrait};

    use super::*;
    use crate::{
        import::{ImportConfig, downloader::SourceDownloader},
        s3::sigv4,
        store::Store,
    };

    const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

    async fn gateway_and_state() -> (GatewayRoute, Arc<AppState>) {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "bucket", None)
            .await
            .unwrap();
        let state = Arc::new(AppState {
            kubo: crate::kubo::KuboClient::new("http://127.0.0.1:1".to_owned()),
            cold_kubo: None,
            store: Store::new(db),
            credentials: HashMap::from([("test".to_owned(), s3s::auth::SecretKey::from("test"))]),
            master_key: crate::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
            pinning: crate::pinning::coordinator::PinningCoordinator::disabled_for_test(),
        });
        let config = ImportConfig::default().validate().unwrap();
        let downloader = SourceDownloader::production(Arc::new(config.clone()));
        let coordinator = ImportCoordinator::new(config, downloader);
        (GatewayRoute::new(state.clone(), coordinator), state)
    }

    async fn gateway() -> GatewayRoute {
        gateway_and_state().await.0
    }

    struct ObservedGatewayRoute {
        gateway: GatewayRoute,
        access_checks: Arc<AtomicUsize>,
        dispatches: Arc<AtomicUsize>,
        allow_access: bool,
    }

    #[async_trait::async_trait]
    impl S3Route for ObservedGatewayRoute {
        fn is_match(
            &self,
            method: &Method,
            uri: &Uri,
            headers: &HeaderMap,
            extensions: &mut http::Extensions,
        ) -> bool {
            self.gateway.is_match(method, uri, headers, extensions)
        }

        async fn check_access(&self, _req: &mut S3Request<Body>) -> S3Result<()> {
            self.access_checks.fetch_add(1, Ordering::SeqCst);
            if self.allow_access {
                Ok(())
            } else {
                Err(s3s::s3_error!(AccessDenied, "test access denial"))
            }
        }

        async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            self.gateway.call(req).await
        }
    }

    async fn call_through_s3_service(
        method: Method,
        key: &str,
        query: &[(&str, &str)],
        headers: HeaderMap,
        body: Vec<u8>,
        allow_access: bool,
    ) -> (StatusCode, usize, usize, Arc<AppState>) {
        let (gateway, state) = gateway_and_state().await;
        let access_checks = Arc::new(AtomicUsize::new(0));
        let dispatches = Arc::new(AtomicUsize::new(0));
        let mut builder = S3ServiceBuilder::new(crate::s3::handler::S3Impl::new(state.clone()));
        builder.set_auth(crate::auth::GatewayAuth::new(state.clone()));
        builder.set_route(ObservedGatewayRoute {
            gateway,
            access_checks: access_checks.clone(),
            dispatches: dispatches.clone(),
            allow_access,
        });
        let app = axum::Router::new().fallback_service(HandleError::new(
            builder.build(),
            |_: s3s::HttpError| async {
                http::Response::builder()
                    .status(500)
                    .body(s3s::Body::from("error".to_owned()))
                    .unwrap()
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let response = sigv4::send_sigv4(
            method, &endpoint, "bucket", key, query, body, headers, "test",
        )
        .await;
        server.abort();
        (
            response.status(),
            access_checks.load(Ordering::SeqCst),
            dispatches.load(Ordering::SeqCst),
            state,
        )
    }

    fn request(method: Method, uri: &str, body: Body) -> S3Request<Body> {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/xml"),
        );
        S3Request {
            input: body,
            method,
            uri: uri.parse().unwrap(),
            headers,
            extensions: http::Extensions::new(),
            credentials: Some(s3s::auth::Credentials {
                access_key: "test".to_owned(),
                secret_key: s3s::auth::SecretKey::from("test"),
            }),
            region: Some("us-east-1".parse().unwrap()),
            service: Some("s3".to_owned()),
            trailing_headers: None,
        }
    }

    #[tokio::test]
    async fn route_predicates_claim_exact_import_and_existing_decompress_shapes_only() {
        let gateway = gateway().await;
        let mut extensions = http::Extensions::new();
        for (method, uri) in [
            (Method::POST, "/bucket/key?ipfs3-import"),
            (Method::GET, "/bucket/key?ipfs3-import=job"),
            (Method::PUT, "/bucket/key?decompress-zip=prefix"),
            (Method::POST, "/bucket/key?uploadId=id"),
        ] {
            assert!(gateway.is_match(
                &method,
                &uri.parse().unwrap(),
                &HeaderMap::new(),
                &mut extensions,
            ));
        }
        for (method, uri) in [
            (Method::PUT, "/bucket/key?ipfs3-import"),
            (Method::POST, "/bucket/key?uploads"),
            (Method::GET, "/bucket/key?unrelated=true"),
            (Method::PUT, "/bucket/key"),
        ] {
            assert!(!gateway.is_match(
                &method,
                &uri.parse().unwrap(),
                &HeaderMap::new(),
                &mut extensions,
            ));
        }
    }

    #[tokio::test]
    async fn import_wins_when_import_and_decompress_are_both_present() {
        let gateway = gateway().await;
        let response = gateway
            .call(request(
                Method::POST,
                "/bucket/archive.zip?ipfs3-import&decompress-zip=prefix%2F",
                Body::from(format!(
                    "<IPFS3ImportRequest><CID>{CID}</CID></IPFS3ImportRequest>"
                )),
            ))
            .await
            .unwrap();

        assert_eq!(response.status, Some(StatusCode::ACCEPTED));
        assert!(response.headers.contains_key("x-ipfs3-import-job-id"));
    }

    #[tokio::test]
    async fn real_s3_service_checks_gateway_access_once_and_dispatches_authenticated_children() {
        let mut import_headers = HeaderMap::new();
        import_headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/xml"),
        );
        let (status, access_checks, dispatches, _) = call_through_s3_service(
            Method::POST,
            "key",
            &[("ipfs3-import", "")],
            import_headers,
            format!("<IPFS3ImportRequest><CID>{CID}</CID></IPFS3ImportRequest>").into_bytes(),
            true,
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(access_checks, 1);
        assert_eq!(dispatches, 1);

        let mut decompress_headers = HeaderMap::new();
        decompress_headers.insert(
            "x-amz-server-side-encryption",
            http::HeaderValue::from_static("AES256"),
        );
        let (status, access_checks, dispatches, _) = call_through_s3_service(
            Method::PUT,
            "archive.zip",
            &[("decompress-zip", "prefix/")],
            decompress_headers,
            b"archive bytes".to_vec(),
            true,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(access_checks, 1);
        assert_eq!(dispatches, 1);

        let (status, access_checks, dispatches, _) = call_through_s3_service(
            Method::GET,
            "",
            &[("list-type", "2")],
            HeaderMap::new(),
            Vec::new(),
            true,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(access_checks, 0);
        assert_eq!(dispatches, 0);
    }

    #[tokio::test]
    async fn real_s3_service_denial_precedes_gateway_dispatch_and_import_work() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/xml"),
        );
        let (status, access_checks, dispatches, state) = call_through_s3_service(
            Method::POST,
            "key",
            &[("ipfs3-import", "")],
            headers,
            b"must not be parsed".to_vec(),
            false,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(access_checks, 1);
        assert_eq!(dispatches, 0);
        assert_eq!(
            crate::store::entities::import_job::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
    }
}
