use std::sync::Arc;

use axum::{
    body::{Body, to_bytes},
    extract::{Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use http::{
    HeaderMap, HeaderName, HeaderValue, Method, StatusCode,
    header::{
        ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_HEADERS,
        ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS,
        ACCESS_CONTROL_MAX_AGE, ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD,
        CONTENT_TYPE, ORIGIN, VARY,
    },
};
use s3s::path::parse_path_style;

use crate::{
    cors::{
        CorsPutBodyMetadata, Crc64NvmeHeader, MAX_CORS_CONFIGURATION_BYTES,
        SdkChecksumAlgorithmHeader,
        checksum::crc64nvme,
        config::from_canonical_json,
        matcher::{AllowOrigin, CorsMatch, first_match},
        model::CorsRule,
    },
    error::AppResult,
    state::AppState,
    store::cors_config::get_optional_configuration,
};

const INVALID_REQUEST_XML: &str = concat!(
    r#"<?xml version="1.0" encoding="UTF-8"?>"#,
    "<Error><Code>InvalidRequest</Code>",
    "<Message>invalid CORS request</Message></Error>"
);
const FORBIDDEN_XML: &str = concat!(
    r#"<?xml version="1.0" encoding="UTF-8"?>"#,
    "<Error><Code>AccessForbidden</Code>",
    "<Message>CORS request forbidden</Message></Error>"
);
const INTERNAL_ERROR_XML: &str = concat!(
    r#"<?xml version="1.0" encoding="UTF-8"?>"#,
    "<Error><Code>InternalError</Code>",
    "<Message>internal server error</Message></Error>"
);

struct BucketPath {
    bucket: String,
    bucket_only: bool,
}

struct RequestedHeaders {
    names: Vec<HeaderName>,
    original: Vec<String>,
    supplied: bool,
}

enum ResponsePolicy<'a> {
    Preflight {
        requested_method: &'a Method,
        requested_headers: &'a RequestedHeaders,
    },
    Actual,
}

/// Applies a bucket's saved browser CORS policy around the authenticated S3 service.
pub async fn bucket_cors(
    State(state): State<Arc<AppState>>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    if matches!(request.uri().path(), "/health" | "/ready") {
        return next.run(request).await;
    }

    let Some(bucket_path) = classify_bucket_path(request.uri().path()) else {
        return next.run(request).await;
    };

    let raw_configuration = match read_configuration(&state, &bucket_path.bucket).await {
        Ok(configuration) => configuration,
        Err(_) => return internal_error(),
    };
    let configuration = match raw_configuration {
        Some(raw) => match from_canonical_json(&raw) {
            Ok(configuration) => Some(configuration),
            Err(_) => return internal_error(),
        },
        None => None,
    };

    let mut request = request;
    if is_put_bucket_cors(&request, bucket_path.bucket_only) {
        request = match reconstruct_put_body(request).await {
            Ok(request) => request,
            Err(response) => return response,
        };
    }

    let origin_present = request.headers().contains_key(ORIGIN);
    let requested_method_present = request
        .headers()
        .contains_key(ACCESS_CONTROL_REQUEST_METHOD);

    if request.method() == Method::OPTIONS {
        if !origin_present && !requested_method_present {
            return next.run(request).await;
        }
        if origin_present != requested_method_present {
            return forbidden();
        }

        let Some(origin_value) = single_header(request.headers(), ORIGIN) else {
            return forbidden();
        };
        let Some(origin) = origin_value.to_str().ok() else {
            return forbidden();
        };
        let Some(requested_method_value) =
            single_header(request.headers(), ACCESS_CONTROL_REQUEST_METHOD)
        else {
            return forbidden();
        };
        let Some(requested_method) = parse_allowed_method(requested_method_value) else {
            return forbidden();
        };
        let Some(requested_headers) = parse_requested_headers(request.headers()) else {
            return forbidden();
        };
        let Some(configuration) = configuration.as_ref() else {
            return forbidden();
        };
        let Some(matched) = first_match(
            configuration,
            origin,
            &requested_method,
            &requested_headers.names,
        ) else {
            return forbidden();
        };
        let Ok(headers) = build_headers(
            matched,
            origin_value,
            ResponsePolicy::Preflight {
                requested_method: &requested_method,
                requested_headers: &requested_headers,
            },
        ) else {
            return forbidden();
        };

        let mut response = StatusCode::OK.into_response();
        append_headers(&mut response, &headers);
        return response;
    }

    let additions = actual_headers(request.headers(), request.method(), configuration.as_ref());
    let mut response = next.run(request).await;
    if let Some(headers) = additions {
        append_headers(&mut response, &headers);
    }
    response
}

fn classify_bucket_path(path: &str) -> Option<BucketPath> {
    let raw_path = path.strip_prefix('/')?;
    if raw_path.is_empty() {
        return None;
    }
    let (raw_bucket, raw_object) = match raw_path.split_once('/') {
        Some((bucket, object)) => (bucket, Some(object)),
        None => (raw_path, None),
    };
    if raw_bucket.is_empty() {
        return None;
    }

    let bucket = percent_encoding::percent_decode_str(raw_bucket)
        .decode_utf8()
        .ok()?;
    if bucket.is_empty() || bucket.contains(['/', '\\']) {
        return None;
    }
    parse_path_style(&format!("/{bucket}")).ok()?.as_bucket()?;

    Some(BucketPath {
        bucket: bucket.into_owned(),
        bucket_only: raw_object.is_none_or(str::is_empty),
    })
}

fn is_put_bucket_cors(request: &Request, bucket_only: bool) -> bool {
    bucket_only
        && request.method() == Method::PUT
        && crate::s3::query::query_key_is_present(request.uri(), "cors")
}

#[allow(clippy::result_large_err)]
async fn reconstruct_put_body(request: Request) -> Result<Request, Response> {
    let (mut parts, body) = request.into_parts();
    let bytes = match to_bytes(body, MAX_CORS_CONFIGURATION_BYTES + 1).await {
        Ok(bytes) if bytes.len() <= MAX_CORS_CONFIGURATION_BYTES => bytes,
        _ => return Err(invalid_request()),
    };
    let sdk_checksum_algorithm = bridge_sdk_checksum_algorithm(&mut parts.headers);
    parts.extensions.insert(CorsPutBodyMetadata {
        len: bytes.len(),
        computed_md5: md5::compute(&bytes).0,
        computed_crc64nvme: crc64nvme(&bytes),
        sdk_checksum_algorithm,
        supplied_crc64nvme: parse_crc64nvme_header(&parts.headers),
    });
    Ok(Request::from_parts(parts, Body::from(bytes)))
}

fn bridge_sdk_checksum_algorithm(headers: &mut HeaderMap) -> SdkChecksumAlgorithmHeader {
    let mut values = headers.get_all("x-amz-sdk-checksum-algorithm").iter();
    let sdk_checksum_algorithm = values.next().cloned();
    let cardinality = match (sdk_checksum_algorithm.as_ref(), values.next()) {
        (None, _) => SdkChecksumAlgorithmHeader::Absent,
        (Some(_), None) => SdkChecksumAlgorithmHeader::Single,
        (Some(_), Some(_)) => SdkChecksumAlgorithmHeader::Invalid,
    };

    headers.remove("x-amz-checksum-algorithm");
    if let (Some(value), SdkChecksumAlgorithmHeader::Single) = (sdk_checksum_algorithm, cardinality)
    {
        headers.insert("x-amz-checksum-algorithm", value);
    }
    cardinality
}

fn parse_crc64nvme_header(headers: &HeaderMap) -> Crc64NvmeHeader {
    let mut values = headers.get_all("x-amz-checksum-crc64nvme").iter();
    let Some(value) = values.next() else {
        return Crc64NvmeHeader::Absent;
    };
    if values.next().is_some() || value.to_str().is_err() {
        return Crc64NvmeHeader::Invalid;
    }
    STANDARD
        .decode(value.as_bytes())
        .ok()
        .and_then(|value| value.try_into().ok())
        .map_or(Crc64NvmeHeader::Invalid, Crc64NvmeHeader::Value)
}

fn actual_headers(
    headers: &HeaderMap,
    method: &Method,
    configuration: Option<&crate::cors::model::CorsConfiguration>,
) -> Option<HeaderMap> {
    let origin_value = single_header(headers, ORIGIN)?;
    let origin = origin_value.to_str().ok()?;

    let matched = first_match(configuration?, origin, method, &[])?;
    build_headers(matched, origin_value, ResponsePolicy::Actual).ok()
}

fn single_header(headers: &HeaderMap, name: HeaderName) -> Option<&HeaderValue> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?;
    values.next().is_none().then_some(value)
}

fn parse_allowed_method(value: &HeaderValue) -> Option<Method> {
    let method = Method::from_bytes(value.as_bytes()).ok()?;
    matches!(
        method,
        Method::GET | Method::PUT | Method::HEAD | Method::POST | Method::DELETE
    )
    .then_some(method)
}

fn parse_requested_headers(headers: &HeaderMap) -> Option<RequestedHeaders> {
    let supplied = headers.contains_key(ACCESS_CONTROL_REQUEST_HEADERS);
    let mut names = Vec::new();
    let mut original = Vec::new();
    for value in headers.get_all(ACCESS_CONTROL_REQUEST_HEADERS) {
        for raw in value.as_bytes().split(|byte| *byte == b',') {
            let trimmed = trim_ows(raw);
            if trimmed.is_empty() {
                return None;
            }
            names.push(HeaderName::from_bytes(trimmed).ok()?);
            original.push(std::str::from_utf8(trimmed).ok()?.to_owned());
        }
    }
    Some(RequestedHeaders {
        names,
        original,
        supplied,
    })
}

fn trim_ows(mut value: &[u8]) -> &[u8] {
    while matches!(value.first(), Some(b' ' | b'\t')) {
        value = &value[1..];
    }
    while matches!(value.last(), Some(b' ' | b'\t')) {
        value = &value[..value.len() - 1];
    }
    value
}

fn build_headers(
    matched: CorsMatch<'_>,
    origin: &HeaderValue,
    policy: ResponsePolicy<'_>,
) -> Result<HeaderMap, ()> {
    let mut headers = HeaderMap::new();
    match matched.allow_origin {
        AllowOrigin::Any => {
            headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
        }
        AllowOrigin::Echo => {
            headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin.clone());
            headers.insert(
                ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            );
        }
    }

    match policy {
        ResponsePolicy::Preflight {
            requested_method,
            requested_headers,
        } => {
            headers.insert(
                ACCESS_CONTROL_ALLOW_METHODS,
                HeaderValue::from_str(requested_method.as_str()).map_err(|_| ())?,
            );
            if requested_headers.supplied {
                headers.insert(
                    ACCESS_CONTROL_ALLOW_HEADERS,
                    HeaderValue::from_str(&requested_headers.original.join(", "))
                        .map_err(|_| ())?,
                );
            }
            if let Some(max_age) = matched.rule.max_age_seconds {
                headers.insert(
                    ACCESS_CONTROL_MAX_AGE,
                    HeaderValue::from_str(&max_age.to_string()).map_err(|_| ())?,
                );
            }
        }
        ResponsePolicy::Actual => append_expose_headers(&mut headers, matched.rule)?,
    }

    for value in [
        HeaderValue::from_static("Origin"),
        HeaderValue::from_static("Access-Control-Request-Method"),
        HeaderValue::from_static("Access-Control-Request-Headers"),
    ] {
        headers.append(VARY, value);
    }
    Ok(headers)
}

fn append_expose_headers(headers: &mut HeaderMap, rule: &CorsRule) -> Result<(), ()> {
    if !rule.expose_headers.is_empty() {
        headers.insert(
            ACCESS_CONTROL_EXPOSE_HEADERS,
            HeaderValue::from_str(&rule.expose_headers.join(", ")).map_err(|_| ())?,
        );
    }
    Ok(())
}

fn append_headers(response: &mut Response, additions: &HeaderMap) {
    for (name, value) in additions {
        response.headers_mut().append(name, value.clone());
    }
}

async fn read_configuration(state: &AppState, bucket: &str) -> AppResult<Option<String>> {
    #[cfg(test)]
    read_observation::record();
    get_optional_configuration(state.store.db(), bucket).await
}

fn fixed_error(status: StatusCode, body: &'static str) -> Response {
    let mut response = (status, body).into_response();
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    response
}

fn invalid_request() -> Response {
    fixed_error(StatusCode::BAD_REQUEST, INVALID_REQUEST_XML)
}

fn forbidden() -> Response {
    fixed_error(StatusCode::FORBIDDEN, FORBIDDEN_XML)
}

fn internal_error() -> Response {
    fixed_error(StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_ERROR_XML)
}

#[cfg(test)]
mod read_observation {
    use std::{
        future::Future,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    tokio::task_local! {
        static READS: Arc<AtomicUsize>;
    }

    pub fn record() {
        let _ = READS.try_with(|reads| {
            reads.fetch_add(1, Ordering::SeqCst);
        });
    }

    pub async fn scope<T>(reads: Arc<AtomicUsize>, future: impl Future<Output = T>) -> T {
        READS.scope(reads, future).await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        io,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use axum::{Router, body::to_bytes, middleware::from_fn_with_state};
    use bytes::Bytes;
    use http::{Request, header::HOST};
    use sea_orm::Database;
    use tower::ServiceExt as _;

    use super::*;
    use crate::{
        cors::{Crc64NvmeHeader, config::canonical_json, model::CorsConfiguration},
        crypto::key::MasterKey,
        kubo::KuboClient,
        pinning::coordinator::PinningCoordinator,
        store::{Store, cors_config::put_configuration},
    };

    struct ObservedRequest {
        headers: HeaderMap,
        body: Result<Vec<u8>, ()>,
        metadata: Option<CorsPutBodyMetadata>,
    }

    struct InnerObservation {
        calls: AtomicUsize,
        requests: Mutex<Vec<ObservedRequest>>,
        status: StatusCode,
        headers: HeaderMap,
    }

    impl InnerObservation {
        fn new(status: StatusCode, headers: HeaderMap) -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
                status,
                headers,
            })
        }

        fn ok() -> Arc<Self> {
            Self::new(StatusCode::OK, HeaderMap::new())
        }
    }

    async fn observed_inner(request: Request<Body>) -> Response {
        let observation = request
            .extensions()
            .get::<Arc<InnerObservation>>()
            .cloned()
            .unwrap();
        let metadata = request.extensions().get::<CorsPutBodyMetadata>().copied();
        let headers = request.headers().clone();
        observation.calls.fetch_add(1, Ordering::SeqCst);
        let body = to_bytes(request.into_body(), usize::MAX)
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|_| ());
        observation.requests.lock().unwrap().push(ObservedRequest {
            headers,
            body,
            metadata,
        });

        let mut response = Response::new(Body::from("inner response"));
        *response.status_mut() = observation.status;
        *response.headers_mut() = observation.headers.clone();
        response
    }

    async fn test_state(raw_configuration: Option<&str>) -> Arc<AppState> {
        test_state_for_bucket("bucket", raw_configuration).await
    }

    async fn test_state_for_bucket(bucket: &str, raw_configuration: Option<&str>) -> Arc<AppState> {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, bucket, None)
            .await
            .unwrap();
        if let Some(raw) = raw_configuration {
            put_configuration(&db, bucket, raw).await.unwrap();
        }
        Arc::new(AppState {
            kubo: KuboClient::new("http://127.0.0.1:1".to_owned()),
            cold_kubo: None,
            store: Store::new(db),
            credentials: HashMap::new(),
            master_key: MasterKey::from_hex(&"0".repeat(64)).unwrap(),
            pinning: PinningCoordinator::disabled_for_test(),
        })
    }

    fn rule(
        origins: &[&str],
        methods: &[&str],
        allowed_headers: &[&str],
        expose_headers: &[&str],
        max_age_seconds: Option<i32>,
    ) -> CorsRule {
        CorsRule {
            allowed_origins: origins.iter().map(|value| (*value).to_owned()).collect(),
            allowed_methods: methods.iter().map(|value| (*value).to_owned()).collect(),
            allowed_headers: allowed_headers
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            expose_headers: expose_headers
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            id: None,
            max_age_seconds,
        }
    }

    fn policy(rules: Vec<CorsRule>) -> String {
        canonical_json(&CorsConfiguration { rules }).unwrap()
    }

    fn request(
        method: Method,
        uri: &str,
        headers: HeaderMap,
        body: Body,
        observation: Arc<InnerObservation>,
    ) -> Request<Body> {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .body(body)
            .unwrap();
        *request.headers_mut() = headers;
        request.extensions_mut().insert(observation);
        request
    }

    async fn call(state: Arc<AppState>, request: Request<Body>) -> (Response, usize) {
        let reads = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .fallback(observed_inner)
            .layer(from_fn_with_state(state, bucket_cors));
        let response = read_observation::scope(reads.clone(), app.oneshot(request))
            .await
            .unwrap();
        (response, reads.load(Ordering::SeqCst))
    }

    fn cors_headers(response: &Response) -> Vec<String> {
        response
            .headers()
            .keys()
            .filter(|name| name.as_str().starts_with("access-control-"))
            .map(|name| name.as_str().to_owned())
            .collect()
    }

    fn values(headers: &HeaderMap, name: HeaderName) -> Vec<String> {
        headers
            .get_all(name)
            .iter()
            .map(|value| value.to_str().unwrap().to_owned())
            .collect()
    }

    async fn body_text(response: Response) -> String {
        String::from_utf8(
            to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap()
    }

    fn preflight_headers(origin: HeaderValue, method: HeaderValue) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, origin);
        headers.insert(ACCESS_CONTROL_REQUEST_METHOD, method);
        headers
    }

    #[tokio::test]
    async fn put_cors_reconstructs_exact_limit_bytes_and_private_metadata() {
        let state = test_state(None).await;
        let observation = InnerObservation::ok();
        let prefix = " \n<CORSRule>Grüße 東京</CORSRule>\t ".as_bytes();
        let mut bytes = vec![b'x'; MAX_CORS_CONFIGURATION_BYTES];
        bytes[..prefix.len()].copy_from_slice(prefix);
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-sdk-checksum-algorithm",
            HeaderValue::from_static("CRC32"),
        );
        let (response, reads) = call(
            state,
            request(
                Method::PUT,
                "/bucket?c%6Frs=value",
                headers,
                Body::from(bytes.clone()),
                observation.clone(),
            ),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(reads, 1);
        assert_eq!(observation.calls.load(Ordering::SeqCst), 1);
        let requests = observation.requests.lock().unwrap();
        assert_eq!(requests[0].body.as_ref().unwrap(), &bytes);
        let metadata = requests[0].metadata.unwrap();
        assert_eq!(metadata.len, bytes.len());
        assert_eq!(metadata.computed_md5, md5::compute(&bytes).0);
        assert_eq!(metadata.computed_crc64nvme, crc64nvme(&bytes));
        assert_eq!(metadata.supplied_crc64nvme, Crc64NvmeHeader::Absent);
    }

    #[tokio::test]
    async fn put_cors_parses_crc64nvme_metadata_and_reconstructs_exact_body() {
        let body = b"private CORS body bytes".to_vec();
        let expected_checksum = [0xae, 0x8b, 0x14, 0x86, 0x0a, 0x79, 0x98, 0x88];
        let cases = [
            (HeaderMap::new(), Crc64NvmeHeader::Absent),
            (
                {
                    let mut headers = HeaderMap::new();
                    headers.insert(
                        "x-amz-checksum-crc64nvme",
                        HeaderValue::from_static("not-base64!"),
                    );
                    headers
                },
                Crc64NvmeHeader::Invalid,
            ),
            (
                {
                    let mut headers = HeaderMap::new();
                    headers.insert("x-amz-checksum-crc64nvme", HeaderValue::from_static("AQ=="));
                    headers
                },
                Crc64NvmeHeader::Invalid,
            ),
            (
                {
                    let mut headers = HeaderMap::new();
                    headers.append(
                        "x-amz-checksum-crc64nvme",
                        HeaderValue::from_static("rosUhgp5mIg="),
                    );
                    headers.append(
                        "x-amz-checksum-crc64nvme",
                        HeaderValue::from_static("rosUhgp5mIg="),
                    );
                    headers
                },
                Crc64NvmeHeader::Invalid,
            ),
            (
                {
                    let mut headers = HeaderMap::new();
                    headers.insert(
                        "x-amz-checksum-crc64nvme",
                        HeaderValue::from_bytes(b"\xff").unwrap(),
                    );
                    headers
                },
                Crc64NvmeHeader::Invalid,
            ),
            (
                {
                    let mut headers = HeaderMap::new();
                    headers.insert(
                        "x-amz-checksum-crc64nvme",
                        HeaderValue::from_static("rosUhgp5mIg="),
                    );
                    headers
                },
                Crc64NvmeHeader::Value(expected_checksum),
            ),
        ];

        for (headers, supplied_crc64nvme) in cases {
            let observation = InnerObservation::ok();
            let (response, reads) = call(
                test_state(None).await,
                request(
                    Method::PUT,
                    "/bucket?cors",
                    headers,
                    Body::from(body.clone()),
                    observation.clone(),
                ),
            )
            .await;

            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(reads, 1);
            let requests = observation.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].body.as_ref().unwrap(), &body);
            let metadata = requests[0].metadata.unwrap();
            assert_eq!(metadata.len, body.len());
            assert_eq!(metadata.computed_md5, md5::compute(&body).0);
            assert_eq!(metadata.computed_crc64nvme, crc64nvme(&body));
            assert_eq!(metadata.supplied_crc64nvme, supplied_crc64nvme);
        }
    }

    #[tokio::test]
    async fn put_cors_records_sdk_checksum_algorithm_cardinality_and_bridges_only_single() {
        let body = b"private CORS body bytes".to_vec();
        let cases = [
            (
                HeaderMap::new(),
                SdkChecksumAlgorithmHeader::Absent,
                Vec::new(),
            ),
            (
                {
                    let mut headers = HeaderMap::new();
                    headers.insert(
                        "x-amz-sdk-checksum-algorithm",
                        HeaderValue::from_static("CRC64NVME"),
                    );
                    headers
                },
                SdkChecksumAlgorithmHeader::Single,
                vec!["CRC64NVME"],
            ),
            (
                {
                    let mut headers = HeaderMap::new();
                    headers.append(
                        "x-amz-sdk-checksum-algorithm",
                        HeaderValue::from_static("CRC64NVME"),
                    );
                    headers.append(
                        "x-amz-sdk-checksum-algorithm",
                        HeaderValue::from_static("CRC64NVME"),
                    );
                    headers
                },
                SdkChecksumAlgorithmHeader::Invalid,
                Vec::new(),
            ),
        ];

        for (headers, sdk_checksum_algorithm, expected_bridged) in cases {
            let observation = InnerObservation::ok();
            let (response, reads) = call(
                test_state(None).await,
                request(
                    Method::PUT,
                    "/bucket?cors",
                    headers,
                    Body::from(body.clone()),
                    observation.clone(),
                ),
            )
            .await;

            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(reads, 1);
            let requests = observation.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(
                requests[0].metadata.unwrap().sdk_checksum_algorithm,
                sdk_checksum_algorithm
            );
            assert_eq!(
                values(
                    &requests[0].headers,
                    HeaderName::from_static("x-amz-checksum-algorithm")
                ),
                expected_bridged
            );
        }
    }

    #[tokio::test]
    async fn put_cors_limit_and_read_failures_are_fixed_safe_400s() {
        for (body, secret) in [
            (
                Body::from(vec![b's'; MAX_CORS_CONFIGURATION_BYTES + 1]),
                "oversized-secret",
            ),
            (
                Body::from_stream(futures_util::stream::once(async {
                    Err::<Bytes, io::Error>(io::Error::other("private stream failure"))
                })),
                "private stream failure",
            ),
        ] {
            let observation = InnerObservation::ok();
            let (response, reads) = call(
                test_state(None).await,
                request(
                    Method::PUT,
                    "/bucket?cors",
                    HeaderMap::new(),
                    body,
                    observation.clone(),
                ),
            )
            .await;

            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(reads, 1);
            assert_eq!(observation.calls.load(Ordering::SeqCst), 0);
            assert!(cors_headers(&response).is_empty());
            let body = body_text(response).await;
            assert_eq!(body, INVALID_REQUEST_XML);
            assert!(!body.contains(secret));
        }
    }

    #[tokio::test]
    async fn non_management_bodies_are_never_buffered_before_the_inner_service() {
        for (method, uri) in [
            (Method::GET, "/bucket?cors"),
            (Method::PUT, "/bucket?other"),
            (Method::PUT, "/bucket/key?cors"),
        ] {
            let observation = InnerObservation::ok();
            let failing = Body::from_stream(futures_util::stream::once(async {
                Err::<Bytes, io::Error>(io::Error::other("inner-only failure"))
            }));
            let (response, reads) = call(
                test_state(None).await,
                request(method, uri, HeaderMap::new(), failing, observation.clone()),
            )
            .await;

            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            assert_eq!(reads, 1, "{uri}");
            assert_eq!(observation.calls.load(Ordering::SeqCst), 1, "{uri}");
            assert!(observation.requests.lock().unwrap()[0].body.is_err());
            assert!(observation.requests.lock().unwrap()[0].metadata.is_none());
        }
    }

    #[tokio::test]
    async fn classification_is_path_style_only_and_covers_custom_object_routes() {
        for uri in [
            "/bucket",
            "/bucket/key",
            "/buck%65t/object%2Fpart",
            "/bucket/archive.zip?decompress-zip=prefix%2Fnested%2F",
            "/bucket/key?ipfs3-import",
        ] {
            let observation = InnerObservation::ok();
            let (response, reads) = call(
                test_state(None).await,
                request(
                    Method::GET,
                    uri,
                    HeaderMap::new(),
                    Body::empty(),
                    observation.clone(),
                ),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            assert_eq!(reads, 1, "{uri}");
            assert_eq!(observation.calls.load(Ordering::SeqCst), 1, "{uri}");
        }

        for (uri, host) in [
            ("/", None),
            ("/%FF", None),
            ("/", Some("bucket.example.test")),
        ] {
            let observation = InnerObservation::ok();
            let mut headers = HeaderMap::new();
            if let Some(host) = host {
                headers.insert(HOST, HeaderValue::from_str(host).unwrap());
            }
            let (_, reads) = call(
                test_state(None).await,
                request(
                    Method::GET,
                    uri,
                    headers,
                    Body::empty(),
                    observation.clone(),
                ),
            )
            .await;
            assert_eq!(reads, 0, "{uri} {host:?}");
            assert_eq!(observation.calls.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn encoded_bucket_separator_cannot_select_another_buckets_policy() {
        let configuration = policy(vec![rule(
            &["https://app.example"],
            &["GET"],
            &[],
            &["X-Expose"],
            None,
        )]);
        for uri in [
            "/buck%2Fet/key",
            "/buck%5Cet/key",
            "/buck%00et/key",
            "/%FF/key",
            "/ab/key",
        ] {
            let observation = InnerObservation::ok();
            let mut headers = HeaderMap::new();
            headers.insert(ORIGIN, HeaderValue::from_static("https://app.example"));
            let (response, reads) = call(
                test_state_for_bucket("buck", Some(&configuration)).await,
                request(
                    Method::GET,
                    uri,
                    headers,
                    Body::empty(),
                    observation.clone(),
                ),
            )
            .await;

            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            assert_eq!(reads, 0, "smuggled path selected a policy: {uri}");
            assert_eq!(observation.calls.load(Ordering::SeqCst), 1, "{uri}");
            assert!(cors_headers(&response).is_empty(), "{uri}");
            assert!(response.headers().get(VARY).is_none(), "{uri}");
        }
    }

    #[tokio::test]
    async fn safe_encoded_bucket_segment_maps_to_the_decoded_bucket() {
        let configuration = policy(vec![rule(
            &["https://app.example"],
            &["GET"],
            &[],
            &["X-Expose"],
            None,
        )]);
        let observation = InnerObservation::ok();
        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, HeaderValue::from_static("https://app.example"));
        let (response, reads) = call(
            test_state(Some(&configuration)).await,
            request(
                Method::GET,
                "/buck%65t/object%2Fpart",
                headers,
                Body::empty(),
                observation.clone(),
            ),
        )
        .await;

        assert_eq!(reads, 1);
        assert_eq!(observation.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            response.headers()[ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://app.example"
        );
        assert_eq!(
            response.headers()[ACCESS_CONTROL_EXPOSE_HEADERS],
            "X-Expose"
        );
    }

    #[tokio::test]
    async fn exact_health_and_ready_paths_bypass_the_store_and_cors() {
        for uri in ["/health", "/ready", "/health?probe=1", "/ready?probe=1"] {
            let observation = InnerObservation::ok();
            let mut headers = HeaderMap::new();
            headers.insert(ORIGIN, HeaderValue::from_static("https://private.example"));
            let (response, reads) = call(
                test_state(None).await,
                request(
                    Method::GET,
                    uri,
                    headers,
                    Body::empty(),
                    observation.clone(),
                ),
            )
            .await;
            assert_eq!(reads, 0, "{uri}");
            assert_eq!(observation.calls.load(Ordering::SeqCst), 1);
            assert!(cors_headers(&response).is_empty());
            assert!(response.headers().get(VARY).is_none());
        }
    }

    #[tokio::test]
    async fn plain_options_and_actual_requests_take_one_snapshot_and_call_inner_once() {
        for (method, headers) in [
            {
                let mut headers = HeaderMap::new();
                headers.insert(
                    ACCESS_CONTROL_REQUEST_HEADERS,
                    HeaderValue::from_static("invalid plain options token"),
                );
                (Method::OPTIONS, headers)
            },
            {
                let mut headers = HeaderMap::new();
                headers.insert(ORIGIN, HeaderValue::from_static("https://app.example"));
                (Method::GET, headers)
            },
        ] {
            let observation = InnerObservation::ok();
            let (response, reads) = call(
                test_state(None).await,
                request(
                    method,
                    "/bucket/key",
                    headers,
                    Body::empty(),
                    observation.clone(),
                ),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(reads, 1);
            assert_eq!(observation.calls.load(Ordering::SeqCst), 1);
            assert!(cors_headers(&response).is_empty());
        }
    }

    #[tokio::test]
    async fn preflight_rejections_are_fixed_safe_403s_without_inner_or_cors_headers() {
        let matching = policy(vec![rule(
            &["https://app.example"],
            &["GET"],
            &["x-good"],
            &[],
            None,
        )]);
        let cases = vec![
            {
                let mut headers = HeaderMap::new();
                headers.insert(ORIGIN, HeaderValue::from_static("https://private.example"));
                (Some(matching.as_str()), headers, "https://private.example")
            },
            {
                let mut headers = HeaderMap::new();
                headers.insert(
                    ACCESS_CONTROL_REQUEST_METHOD,
                    HeaderValue::from_static("GET"),
                );
                (Some(matching.as_str()), headers, "GET")
            },
            (
                Some(matching.as_str()),
                preflight_headers(
                    HeaderValue::from_bytes(b"https://private.\xff").unwrap(),
                    HeaderValue::from_static("GET"),
                ),
                "private",
            ),
            (
                Some(matching.as_str()),
                preflight_headers(
                    HeaderValue::from_static("https://app.example"),
                    HeaderValue::from_static("PATCH"),
                ),
                "PATCH",
            ),
            (
                Some(matching.as_str()),
                preflight_headers(
                    HeaderValue::from_static("https://app.example"),
                    HeaderValue::from_bytes(b"G\xffT").unwrap(),
                ),
                "app.example",
            ),
            {
                let mut headers = preflight_headers(
                    HeaderValue::from_static("https://app.example"),
                    HeaderValue::from_static("GET"),
                );
                headers.insert(
                    ACCESS_CONTROL_REQUEST_HEADERS,
                    HeaderValue::from_static("X-Good, ,X-Private"),
                );
                (Some(matching.as_str()), headers, "X-Private")
            },
            {
                let mut headers = preflight_headers(
                    HeaderValue::from_static("https://app.example"),
                    HeaderValue::from_static("GET"),
                );
                headers.insert(
                    ACCESS_CONTROL_REQUEST_HEADERS,
                    HeaderValue::from_bytes(b"X-Good,X-\xff").unwrap(),
                );
                (Some(matching.as_str()), headers, "app.example")
            },
            (
                None,
                preflight_headers(
                    HeaderValue::from_static("https://app.example"),
                    HeaderValue::from_static("GET"),
                ),
                "app.example",
            ),
            (
                Some(matching.as_str()),
                preflight_headers(
                    HeaderValue::from_static("https://other.example"),
                    HeaderValue::from_static("GET"),
                ),
                "other.example",
            ),
        ];

        for (configuration, headers, sensitive) in cases {
            let observation = InnerObservation::ok();
            let (response, reads) = call(
                test_state(configuration).await,
                request(
                    Method::OPTIONS,
                    "/bucket/key",
                    headers,
                    Body::empty(),
                    observation.clone(),
                ),
            )
            .await;
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
            assert_eq!(reads, 1);
            assert_eq!(observation.calls.load(Ordering::SeqCst), 0);
            assert!(cors_headers(&response).is_empty());
            assert!(response.headers().get(VARY).is_none());
            let body = body_text(response).await;
            assert_eq!(body, FORBIDDEN_XML);
            assert!(!body.contains(sensitive));
        }
    }

    #[tokio::test]
    async fn valid_preflight_uses_first_rule_and_preserves_requested_header_spelling() {
        let configuration = policy(vec![
            rule(
                &["https://app.example"],
                &["GET"],
                &["x-*"],
                &["X-First-Expose"],
                Some(60),
            ),
            rule(&["*"], &["GET"], &["*"], &["X-Second-Expose"], Some(120)),
        ]);
        let state = test_state(Some(&configuration)).await;
        let observation = InnerObservation::ok();
        let mut headers = preflight_headers(
            HeaderValue::from_static("https://app.example"),
            HeaderValue::from_static("GET"),
        );
        headers.insert(
            ACCESS_CONTROL_REQUEST_HEADERS,
            HeaderValue::from_static(" X-SeCoNd\t, x-FIRST "),
        );
        let (response, reads) = call(
            state,
            request(
                Method::OPTIONS,
                "/bucket/key?decompress-zip=prefix",
                headers,
                Body::empty(),
                observation.clone(),
            ),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(reads, 1);
        assert_eq!(observation.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            response.headers()[ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://app.example"
        );
        assert_eq!(response.headers()[ACCESS_CONTROL_ALLOW_CREDENTIALS], "true");
        assert_eq!(response.headers()[ACCESS_CONTROL_ALLOW_METHODS], "GET");
        assert_eq!(
            response.headers()[ACCESS_CONTROL_ALLOW_HEADERS],
            "X-SeCoNd, x-FIRST"
        );
        assert_eq!(response.headers()[ACCESS_CONTROL_MAX_AGE], "60");
        assert!(
            response
                .headers()
                .get(ACCESS_CONTROL_EXPOSE_HEADERS)
                .is_none()
        );
        assert_eq!(
            values(response.headers(), VARY),
            [
                "Origin",
                "Access-Control-Request-Method",
                "Access-Control-Request-Headers"
            ]
        );
    }

    #[tokio::test]
    async fn wildcard_origin_omits_credentials() {
        let configuration = policy(vec![rule(&["*"], &["GET"], &[], &[], None)]);
        let observation = InnerObservation::ok();
        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, HeaderValue::from_static("https://any.example"));
        let (response, reads) = call(
            test_state(Some(&configuration)).await,
            request(
                Method::GET,
                "/bucket/key",
                headers,
                Body::empty(),
                observation.clone(),
            ),
        )
        .await;

        assert_eq!(reads, 1);
        assert_eq!(observation.calls.load(Ordering::SeqCst), 1);
        assert_eq!(response.headers()[ACCESS_CONTROL_ALLOW_ORIGIN], "*");
        assert!(
            response
                .headers()
                .get(ACCESS_CONTROL_ALLOW_CREDENTIALS)
                .is_none()
        );
    }

    #[tokio::test]
    async fn actual_error_response_gets_expose_and_appended_vary_but_no_preflight_headers() {
        let configuration = policy(vec![
            rule(
                &["https://app.example"],
                &["GET"],
                &[],
                &["X-First-Expose", "x-trace-id"],
                Some(60),
            ),
            rule(
                &["https://app.example"],
                &["GET"],
                &[],
                &["X-Second-Expose"],
                None,
            ),
        ]);
        let mut inner_headers = HeaderMap::new();
        inner_headers.append(VARY, HeaderValue::from_static("Accept-Encoding"));
        let observation = InnerObservation::new(StatusCode::NOT_FOUND, inner_headers);
        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, HeaderValue::from_static("https://app.example"));
        let (response, reads) = call(
            test_state(Some(&configuration)).await,
            request(
                Method::GET,
                "/bucket/key?ipfs3-import",
                headers,
                Body::empty(),
                observation.clone(),
            ),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(reads, 1);
        assert_eq!(observation.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            response.headers()[ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://app.example"
        );
        assert_eq!(
            response.headers()[ACCESS_CONTROL_EXPOSE_HEADERS],
            "X-First-Expose, x-trace-id"
        );
        for absent in [
            ACCESS_CONTROL_ALLOW_METHODS,
            ACCESS_CONTROL_ALLOW_HEADERS,
            ACCESS_CONTROL_MAX_AGE,
        ] {
            assert!(response.headers().get(absent).is_none());
        }
        assert_eq!(
            values(response.headers(), VARY),
            [
                "Accept-Encoding",
                "Origin",
                "Access-Control-Request-Method",
                "Access-Control-Request-Headers"
            ]
        );
    }

    #[tokio::test]
    async fn invalid_actual_origin_forwards_unchanged_after_snapshot() {
        let configuration = policy(vec![rule(
            &["https://app.example"],
            &["GET"],
            &[],
            &["X-Expose"],
            None,
        )]);
        let observation = InnerObservation::ok();
        let mut headers = HeaderMap::new();
        headers.insert(
            ORIGIN,
            HeaderValue::from_bytes(b"https://app.\xff").unwrap(),
        );
        let (response, reads) = call(
            test_state(Some(&configuration)).await,
            request(
                Method::GET,
                "/bucket/key",
                headers,
                Body::empty(),
                observation.clone(),
            ),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(reads, 1);
        assert_eq!(observation.calls.load(Ordering::SeqCst), 1);
        assert!(cors_headers(&response).is_empty());
        assert!(response.headers().get(VARY).is_none());
    }

    #[tokio::test]
    async fn actual_request_ignores_malformed_duplicate_preflight_only_headers() {
        let configuration = policy(vec![rule(
            &["https://app.example"],
            &["GET"],
            &[],
            &["X-Expose"],
            None,
        )]);
        let observation = InnerObservation::ok();
        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, HeaderValue::from_static("https://app.example"));
        headers.append(
            ACCESS_CONTROL_REQUEST_METHOD,
            HeaderValue::from_static("PATCH"),
        );
        headers.append(
            ACCESS_CONTROL_REQUEST_METHOD,
            HeaderValue::from_bytes(b"G\xffT").unwrap(),
        );
        headers.append(
            ACCESS_CONTROL_REQUEST_HEADERS,
            HeaderValue::from_static("x-good,,x-bad"),
        );
        headers.append(
            ACCESS_CONTROL_REQUEST_HEADERS,
            HeaderValue::from_bytes(b"x-\xff").unwrap(),
        );
        let (response, reads) = call(
            test_state(Some(&configuration)).await,
            request(
                Method::GET,
                "/bucket/key",
                headers,
                Body::empty(),
                observation.clone(),
            ),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(reads, 1);
        assert_eq!(observation.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            response.headers()[ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://app.example"
        );
        assert_eq!(
            response.headers()[ACCESS_CONTROL_EXPOSE_HEADERS],
            "X-Expose"
        );
        assert_eq!(
            values(response.headers(), VARY),
            [
                "Origin",
                "Access-Control-Request-Method",
                "Access-Control-Request-Headers"
            ]
        );
    }

    #[tokio::test]
    async fn database_and_corrupt_snapshot_failures_are_fixed_safe_500s() {
        let corrupt = r#"{"rules":[{"allowed_origins":["https://secret.example"],"allowed_methods":["PATCH"],"allowed_headers":["x-private"],"expose_headers":[],"id":null,"max_age_seconds":null}]}"#;
        let corrupt_state = test_state(Some(corrupt)).await;
        let closed_state = test_state(None).await;
        closed_state.store.db().clone().close().await.unwrap();

        for (state, sensitive) in [
            (corrupt_state, "secret.example"),
            (closed_state, "database"),
        ] {
            let observation = InnerObservation::ok();
            let mut headers = HeaderMap::new();
            headers.insert(ORIGIN, HeaderValue::from_static("https://request.example"));
            let (response, reads) = call(
                state,
                request(
                    Method::GET,
                    "/bucket/key",
                    headers,
                    Body::empty(),
                    observation.clone(),
                ),
            )
            .await;

            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(reads, 1);
            assert_eq!(observation.calls.load(Ordering::SeqCst), 0);
            assert!(cors_headers(&response).is_empty());
            assert!(response.headers().get(VARY).is_none());
            let body = body_text(response).await;
            assert_eq!(body, INTERNAL_ERROR_XML);
            assert!(!body.contains(sensitive));
            assert!(!body.contains("request.example"));
        }
    }

    #[test]
    fn failed_header_construction_cannot_escape_partial_additions() {
        let invalid_rule = rule(
            &["https://app.example"],
            &["GET"],
            &[],
            &["bad\nheader"],
            None,
        );
        let matched = CorsMatch {
            rule: &invalid_rule,
            allow_origin: AllowOrigin::Echo,
        };
        assert!(
            build_headers(
                matched,
                &HeaderValue::from_static("https://app.example"),
                ResponsePolicy::Actual,
            )
            .is_err()
        );
    }
}
