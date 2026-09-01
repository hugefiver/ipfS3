#[allow(dead_code)]
#[path = "support/cors.rs"]
mod cors_support;
#[allow(dead_code)]
#[path = "support/sigv4.rs"]
mod sigv4_support;

use base64::Engine as _;
use cors_support::CorsHarness;
use http::{
    HeaderMap, HeaderName, HeaderValue, StatusCode,
    header::{
        ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_HEADERS,
        ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS,
        ACCESS_CONTROL_MAX_AGE, ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD,
        CONTENT_TYPE, ORIGIN, VARY,
    },
};
use ipfs_s3_gateway::{
    cors::{
        config::canonical_json,
        model::{CorsConfiguration, CorsRule},
    },
    store,
};
use sea_orm::{EntityTrait, PaginatorTrait};
use sigv4_support::send_sigv4;

const SECRET: &str = "test";
const EXACT_ORIGIN: &str = "https://app.example";
const PARTIAL_ORIGIN: &str = "https://tenant.example";
const OTHER_ORIGIN: &str = "https://other.invalid";
const PRIVATE_ORIGIN: &str = "https://private-origin.invalid";
const PRIVATE_POLICY_VALUE: &str = "private-policy-value";
const OBJECT_CID: &str = "bafkreib6w4w2wbr3xqv4urhgrzjbnmzm22qz7x4j6x3a7xrdbm4u2vmyuy";
const CONTENT_MD5: HeaderName = HeaderName::from_static("content-md5");
const SDK_CHECKSUM_ALGORITHM: HeaderName = HeaderName::from_static("x-amz-sdk-checksum-algorithm");
const CHECKSUM_CRC64NVME: HeaderName = HeaderName::from_static("x-amz-checksum-crc64nvme");
const CRC64NVME_CRC_ONLY_DIGEST: &str = "7CrgDRqxSwY=";

fn cors_xml(id: &str, origin: &str, method: &str) -> Vec<u8> {
    format!(
        " \n<CORSConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
         <CORSRule><ID>{id}</ID><AllowedOrigin>{origin}</AllowedOrigin>\
         <AllowedMethod>{method}</AllowedMethod><AllowedHeader>X-Request-*</AllowedHeader>\
         <ExposeHeader>X-Response-Id</ExposeHeader><MaxAgeSeconds>60</MaxAgeSeconds>\
         </CORSRule></CORSConfiguration>\t "
    )
    .into_bytes()
}

fn md5_headers(body: &[u8]) -> HeaderMap {
    let digest = base64::engine::general_purpose::STANDARD.encode(md5::compute(body).0);
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_MD5,
        HeaderValue::from_str(&digest).expect("test MD5 is a valid header value"),
    );
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    headers
}

fn crc64nvme_headers(digest: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    headers.insert(
        SDK_CHECKSUM_ALGORITHM,
        HeaderValue::from_static("CRC64NVME"),
    );
    headers.insert(
        CHECKSUM_CRC64NVME,
        HeaderValue::from_str(digest).expect("test CRC64NVME is a valid header value"),
    );
    headers
}

async fn signed_cors_request(
    harness: &CorsHarness,
    method: reqwest::Method,
    bucket: &str,
    body: Vec<u8>,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        method,
        &harness.endpoint,
        bucket,
        "",
        &[("cors", "")],
        body,
        headers,
        SECRET,
    )
    .await
}

async fn signed_put_cors(
    harness: &CorsHarness,
    bucket: &str,
    body: Vec<u8>,
    mut headers: HeaderMap,
) -> reqwest::Response {
    if !headers.contains_key(CONTENT_TYPE) {
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    }
    signed_cors_request(harness, reqwest::Method::PUT, bucket, body, headers).await
}

fn xml_element(body: &[u8], name: &str) -> Option<Vec<u8>> {
    let opening = format!("<{name}>");
    let closing = format!("</{name}>");
    let start = body
        .windows(opening.len())
        .position(|window| window == opening.as_bytes())?
        + opening.len();
    let end = body[start..]
        .windows(closing.len())
        .position(|window| window == closing.as_bytes())?
        + start;
    Some(body[start..end].to_vec())
}

async fn error_parts(response: reqwest::Response) -> (StatusCode, String, Vec<u8>) {
    let status = response.status();
    let body = response
        .bytes()
        .await
        .expect("read fixed negative response body")
        .to_vec();
    let code = xml_element(&body, "Code").expect("negative response has an S3 error code");
    let code = String::from_utf8(code).expect("S3 error code is UTF-8");
    (status, code, body)
}

async fn assert_s3_error(response: reqwest::Response, status: StatusCode, code: &str) {
    let (actual_status, actual_code, _) = error_parts(response).await;
    assert_eq!(actual_status, status);
    assert_eq!(actual_code, code);
}

fn policy(rules: Vec<CorsRule>) -> String {
    canonical_json(&CorsConfiguration { rules }).expect("focused CORS policy is valid")
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

async fn install_policy(harness: &CorsHarness, policy: String) {
    store::cors_config::put_configuration(harness.state.store.db(), &harness.bucket, &policy)
        .await
        .expect("install focused CORS policy");
}

fn preflight_headers(origin: &str, method: &str, requested: Option<&str>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(ORIGIN, HeaderValue::from_str(origin).expect("test origin"));
    headers.insert(
        ACCESS_CONTROL_REQUEST_METHOD,
        HeaderValue::from_str(method).expect("test requested method"),
    );
    if let Some(requested) = requested {
        headers.insert(
            ACCESS_CONTROL_REQUEST_HEADERS,
            HeaderValue::from_str(requested).expect("test requested headers"),
        );
    }
    headers
}

async fn browser_request(
    harness: &CorsHarness,
    method: reqwest::Method,
    path_and_query: &str,
    headers: HeaderMap,
) -> reqwest::Response {
    reqwest::Client::new()
        .request(method, format!("{}{path_and_query}", harness.endpoint))
        .headers(headers)
        .send()
        .await
        .expect("send browser-style request")
}

fn assert_no_cors_headers(response: &reqwest::Response) {
    assert!(
        response
            .headers()
            .keys()
            .all(|name| !name.as_str().starts_with("access-control-"))
    );
}

fn vary_values(response: &reqwest::Response) -> Vec<&str> {
    response
        .headers()
        .get_all(VARY)
        .iter()
        .map(|value| value.to_str().expect("Vary value is text"))
        .collect()
}

async fn side_effect_counts(harness: &CorsHarness) -> (u64, u64, u64) {
    (
        store::entities::object::Entity::find()
            .count(harness.state.store.db())
            .await
            .expect("count objects"),
        store::entities::object_version::Entity::find()
            .count(harness.state.store.db())
            .await
            .expect("count object versions"),
        store::entities::import_job::Entity::find()
            .count(harness.state.store.db())
            .await
            .expect("count import jobs"),
    )
}

#[tokio::test]
async fn signed_management_round_trip_replaces_and_deletes_idempotently() {
    let harness = cors_support::start_harness().await;
    assert_s3_error(
        signed_cors_request(
            &harness,
            reqwest::Method::GET,
            &harness.bucket,
            Vec::new(),
            HeaderMap::new(),
        )
        .await,
        StatusCode::NOT_FOUND,
        "NoSuchCORSConfiguration",
    )
    .await;

    let first = cors_xml("Grüße東京", EXACT_ORIGIN, "GET");
    let put = signed_put_cors(
        &harness,
        &harness.bucket,
        first.clone(),
        md5_headers(&first),
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);

    let get = signed_cors_request(
        &harness,
        reqwest::Method::GET,
        &harness.bucket,
        Vec::new(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(get.status(), StatusCode::OK);
    let body = get.bytes().await.expect("read positive CORS configuration");
    assert_eq!(
        xml_element(&body, "ID").as_deref(),
        Some("Grüße東京".as_bytes())
    );
    assert_eq!(
        xml_element(&body, "AllowedOrigin").as_deref(),
        Some(EXACT_ORIGIN.as_bytes())
    );

    let replacement = cors_xml("replacement", "https://replacement.example", "PUT");
    let replaced = signed_put_cors(
        &harness,
        &harness.bucket,
        replacement.clone(),
        md5_headers(&replacement),
    )
    .await;
    assert_eq!(replaced.status(), StatusCode::OK);
    let get = signed_cors_request(
        &harness,
        reqwest::Method::GET,
        &harness.bucket,
        Vec::new(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(get.status(), StatusCode::OK);
    let body = get.bytes().await.expect("read replaced CORS configuration");
    assert_eq!(
        xml_element(&body, "ID").as_deref(),
        Some(b"replacement".as_slice())
    );

    for _ in 0..2 {
        let delete = signed_cors_request(
            &harness,
            reqwest::Method::DELETE,
            &harness.bucket,
            Vec::new(),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(delete.status(), StatusCode::NO_CONTENT);
    }
    assert_s3_error(
        signed_cors_request(
            &harness,
            reqwest::Method::GET,
            &harness.bucket,
            Vec::new(),
            HeaderMap::new(),
        )
        .await,
        StatusCode::NOT_FOUND,
        "NoSuchCORSConfiguration",
    )
    .await;
}

#[tokio::test]
async fn signed_management_crc64nvme_only_and_combined_proofs_round_trip() {
    let harness = cors_support::start_harness().await;
    let crc_only = cors_xml("crc-only", EXACT_ORIGIN, "GET");
    assert_eq!(
        crc_only,
        b" \n<CORSConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n<CORSRule><ID>crc-only</ID><AllowedOrigin>https://app.example</AllowedOrigin><AllowedMethod>GET</AllowedMethod><AllowedHeader>X-Request-*</AllowedHeader><ExposeHeader>X-Response-Id</ExposeHeader><MaxAgeSeconds>60</MaxAgeSeconds></CORSRule></CORSConfiguration>\t ".to_vec()
    );

    let crc_only_response = signed_put_cors(
        &harness,
        &harness.bucket,
        crc_only.clone(),
        crc64nvme_headers(CRC64NVME_CRC_ONLY_DIGEST),
    )
    .await;
    assert_eq!(crc_only_response.status(), StatusCode::OK);

    let combined = cors_xml("combined", EXACT_ORIGIN, "GET");
    let mut combined_headers = md5_headers(&combined);
    combined_headers.insert(
        SDK_CHECKSUM_ALGORITHM,
        HeaderValue::from_static("CRC64NVME"),
    );
    combined_headers.insert(CHECKSUM_CRC64NVME, HeaderValue::from_static("L9IRxFvTELg="));
    let combined_response =
        signed_put_cors(&harness, &harness.bucket, combined, combined_headers).await;
    assert_eq!(combined_response.status(), StatusCode::OK);
}

#[tokio::test]
async fn signed_management_rejects_digests_checksums_owners_xml_and_tampering() {
    let harness = cors_support::start_harness().await;
    let valid = cors_xml("validation", EXACT_ORIGIN, "GET");

    assert_s3_error(
        signed_put_cors(&harness, &harness.bucket, valid.clone(), HeaderMap::new()).await,
        StatusCode::BAD_REQUEST,
        "InvalidRequest",
    )
    .await;

    for encoded in ["not-base64!", "AQ=="] {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_MD5, HeaderValue::from_static(encoded));
        assert_s3_error(
            signed_put_cors(&harness, &harness.bucket, valid.clone(), headers).await,
            StatusCode::BAD_REQUEST,
            "InvalidDigest",
        )
        .await;
    }

    let mut mismatch = HeaderMap::new();
    mismatch.insert(
        CONTENT_MD5,
        HeaderValue::from_static("AAAAAAAAAAAAAAAAAAAAAA=="),
    );
    assert_s3_error(
        signed_put_cors(&harness, &harness.bucket, valid.clone(), mismatch).await,
        StatusCode::BAD_REQUEST,
        "BadDigest",
    )
    .await;

    for encoded in ["not-base64!", "AQ=="] {
        assert_s3_error(
            signed_put_cors(
                &harness,
                &harness.bucket,
                valid.clone(),
                crc64nvme_headers(encoded),
            )
            .await,
            StatusCode::BAD_REQUEST,
            "InvalidDigest",
        )
        .await;
    }

    assert_s3_error(
        signed_put_cors(
            &harness,
            &harness.bucket,
            valid.clone(),
            crc64nvme_headers("AAAAAAAAAAA="),
        )
        .await,
        StatusCode::BAD_REQUEST,
        "BadDigest",
    )
    .await;

    for mut headers in [
        {
            let mut headers = HeaderMap::new();
            headers.insert(
                SDK_CHECKSUM_ALGORITHM,
                HeaderValue::from_static("CRC64NVME"),
            );
            headers
        },
        {
            let mut headers = HeaderMap::new();
            headers.insert(
                CHECKSUM_CRC64NVME,
                HeaderValue::from_static(CRC64NVME_CRC_ONLY_DIGEST),
            );
            headers
        },
        {
            let mut headers = HeaderMap::new();
            headers.insert(SDK_CHECKSUM_ALGORITHM, HeaderValue::from_static("CRC32"));
            headers.insert(
                CHECKSUM_CRC64NVME,
                HeaderValue::from_static(CRC64NVME_CRC_ONLY_DIGEST),
            );
            headers
        },
    ] {
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
        assert_s3_error(
            signed_put_cors(&harness, &harness.bucket, valid.clone(), headers).await,
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
        )
        .await;
    }

    for algorithm in ["CRC32", "PRIVATE"] {
        let mut headers = md5_headers(&valid);
        headers.insert(
            HeaderName::from_static("x-amz-sdk-checksum-algorithm"),
            HeaderValue::from_static(algorithm),
        );
        assert_s3_error(
            signed_put_cors(&harness, &harness.bucket, valid.clone(), headers).await,
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
        )
        .await;
    }

    let mut owner_match = md5_headers(&valid);
    owner_match.insert(
        HeaderName::from_static("x-amz-expected-bucket-owner"),
        HeaderValue::from_static("owner"),
    );
    assert_eq!(
        signed_put_cors(&harness, &harness.bucket, valid.clone(), owner_match)
            .await
            .status(),
        StatusCode::OK
    );
    let mut owner_mismatch = HeaderMap::new();
    owner_mismatch.insert(
        HeaderName::from_static("x-amz-expected-bucket-owner"),
        HeaderValue::from_static("other-owner"),
    );
    assert_s3_error(
        signed_cors_request(
            &harness,
            reqwest::Method::GET,
            &harness.bucket,
            Vec::new(),
            owner_mismatch,
        )
        .await,
        StatusCode::FORBIDDEN,
        "AccessDenied",
    )
    .await;
    assert_s3_error(
        signed_put_cors(
            &harness,
            "missing-bucket",
            valid.clone(),
            md5_headers(&valid),
        )
        .await,
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
    )
    .await;

    let malformed = b"<CORSConfiguration><CORSRule>".to_vec();
    assert_s3_error(
        signed_put_cors(
            &harness,
            &harness.bucket,
            malformed.clone(),
            md5_headers(&malformed),
        )
        .await,
        StatusCode::BAD_REQUEST,
        "MalformedXML",
    )
    .await;
    let semantic = cors_xml("semantic", EXACT_ORIGIN, "get");
    assert_s3_error(
        signed_put_cors(
            &harness,
            &harness.bucket,
            semantic.clone(),
            md5_headers(&semantic),
        )
        .await,
        StatusCode::BAD_REQUEST,
        "InvalidRequest",
    )
    .await;

    let tampered = format!(
        "{{\"rules\":[{{\"allowed_origins\":[\"{PRIVATE_ORIGIN}\"],\"allowed_methods\":[\"get\"],\"allowed_headers\":[],\"expose_headers\":[],\"id\":\"{PRIVATE_POLICY_VALUE}\",\"max_age_seconds\":null}}]}}"
    );
    store::cors_config::put_configuration(harness.state.store.db(), &harness.bucket, &tampered)
        .await
        .expect("install deliberately corrupt stored CORS policy");
    let (status, code, body) = error_parts(
        signed_cors_request(
            &harness,
            reqwest::Method::GET,
            &harness.bucket,
            Vec::new(),
            HeaderMap::new(),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(code, "InternalError");
    assert!(
        !body
            .windows(PRIVATE_ORIGIN.len())
            .any(|value| value == PRIVATE_ORIGIN.as_bytes())
    );
    assert!(
        !body
            .windows(PRIVATE_POLICY_VALUE.len())
            .any(|value| value == PRIVATE_POLICY_VALUE.as_bytes())
    );
}

#[tokio::test]
async fn signed_management_rejects_duplicate_sdk_checksum_algorithm_with_valid_md5() {
    let harness = cors_support::start_harness().await;
    let body = cors_xml("duplicate-sdk-checksum", EXACT_ORIGIN, "GET");
    let mut headers = md5_headers(&body);
    headers.append(
        SDK_CHECKSUM_ALGORITHM,
        HeaderValue::from_static("CRC64NVME"),
    );
    headers.append(
        SDK_CHECKSUM_ALGORITHM,
        HeaderValue::from_static("CRC64NVME"),
    );

    let response = signed_put_cors(&harness, &harness.bucket, body, headers).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_s3_error(response, StatusCode::BAD_REQUEST, "InvalidRequest").await;
}

#[tokio::test]
async fn unsigned_browser_preflight_matrix_is_enforced_before_authentication() {
    let harness = cors_support::start_harness().await;
    let object_path = format!("/{}/object.txt", harness.bucket);

    let no_config = browser_request(
        &harness,
        reqwest::Method::OPTIONS,
        &object_path,
        preflight_headers(EXACT_ORIGIN, "GET", None),
    )
    .await;
    assert_eq!(no_config.status(), StatusCode::FORBIDDEN);
    assert_no_cors_headers(&no_config);

    install_policy(
        &harness,
        policy(vec![
            rule(
                &[EXACT_ORIGIN],
                &["GET"],
                &["x-request-*"],
                &["X-First-Expose"],
                Some(60),
            ),
            rule(
                &["https://*.example"],
                &["GET", "PUT"],
                &["x-anything"],
                &["X-Second-Expose"],
                Some(120),
            ),
        ]),
    )
    .await;
    let exact = browser_request(
        &harness,
        reqwest::Method::OPTIONS,
        &object_path,
        preflight_headers(EXACT_ORIGIN, "GET", Some(" X-Request-B , x-request-A ")),
    )
    .await;
    assert_eq!(exact.status(), StatusCode::OK);
    assert_eq!(exact.headers()[ACCESS_CONTROL_ALLOW_ORIGIN], EXACT_ORIGIN);
    assert_eq!(exact.headers()[ACCESS_CONTROL_ALLOW_CREDENTIALS], "true");
    assert_eq!(exact.headers()[ACCESS_CONTROL_ALLOW_METHODS], "GET");
    assert_eq!(
        exact.headers()[ACCESS_CONTROL_ALLOW_HEADERS],
        "X-Request-B, x-request-A"
    );
    assert_eq!(exact.headers()[ACCESS_CONTROL_MAX_AGE], "60");
    assert!(!exact.headers().contains_key(ACCESS_CONTROL_EXPOSE_HEADERS));
    assert_eq!(
        vary_values(&exact),
        [
            "Origin",
            "Access-Control-Request-Method",
            "Access-Control-Request-Headers"
        ]
    );

    let partial = browser_request(
        &harness,
        reqwest::Method::OPTIONS,
        &object_path,
        preflight_headers(PARTIAL_ORIGIN, "PUT", Some("X-Anything")),
    )
    .await;
    assert_eq!(partial.status(), StatusCode::OK);
    assert_eq!(
        partial.headers()[ACCESS_CONTROL_ALLOW_ORIGIN],
        PARTIAL_ORIGIN
    );
    assert_eq!(partial.headers()[ACCESS_CONTROL_MAX_AGE], "120");

    for headers in [
        preflight_headers(OTHER_ORIGIN, "GET", None),
        preflight_headers(EXACT_ORIGIN, "DELETE", None),
        preflight_headers(EXACT_ORIGIN, "GET", Some("X-Disallowed")),
        preflight_headers(EXACT_ORIGIN, "GET", Some("X-Request-A, bad header")),
    ] {
        let response =
            browser_request(&harness, reqwest::Method::OPTIONS, &object_path, headers).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_no_cors_headers(&response);
    }

    let mut duplicate_origin = preflight_headers(EXACT_ORIGIN, "GET", None);
    duplicate_origin.append(
        ORIGIN,
        HeaderValue::from_static("https://duplicate.example"),
    );
    let duplicate = browser_request(
        &harness,
        reqwest::Method::OPTIONS,
        &object_path,
        duplicate_origin,
    )
    .await;
    assert_eq!(duplicate.status(), StatusCode::FORBIDDEN);
    assert_no_cors_headers(&duplicate);

    let mut duplicate_method = preflight_headers(EXACT_ORIGIN, "GET", None);
    duplicate_method.append(
        ACCESS_CONTROL_REQUEST_METHOD,
        HeaderValue::from_static("PUT"),
    );
    let duplicate = browser_request(
        &harness,
        reqwest::Method::OPTIONS,
        &object_path,
        duplicate_method,
    )
    .await;
    assert_eq!(duplicate.status(), StatusCode::FORBIDDEN);
    assert_no_cors_headers(&duplicate);

    for headers in [
        {
            let mut headers = HeaderMap::new();
            headers.insert(ORIGIN, HeaderValue::from_static(EXACT_ORIGIN));
            headers
        },
        {
            let mut headers = HeaderMap::new();
            headers.insert(
                ACCESS_CONTROL_REQUEST_METHOD,
                HeaderValue::from_static("GET"),
            );
            headers
        },
        HeaderMap::new(),
    ] {
        let response =
            browser_request(&harness, reqwest::Method::OPTIONS, &object_path, headers).await;
        assert_ne!(response.status(), StatusCode::OK);
        assert_no_cors_headers(&response);
    }

    install_policy(
        &harness,
        policy(vec![rule(&["*"], &["GET"], &[], &[], None)]),
    )
    .await;
    let wildcard = browser_request(
        &harness,
        reqwest::Method::OPTIONS,
        &object_path,
        preflight_headers(OTHER_ORIGIN, "GET", None),
    )
    .await;
    assert_eq!(wildcard.status(), StatusCode::OK);
    assert_eq!(wildcard.headers()[ACCESS_CONTROL_ALLOW_ORIGIN], "*");
    assert!(
        !wildcard
            .headers()
            .contains_key(ACCESS_CONTROL_ALLOW_CREDENTIALS)
    );
}

#[tokio::test]
async fn signed_actual_custom_routes_and_probes_use_the_production_stack() {
    let harness = cors_support::start_harness().await;
    install_policy(
        &harness,
        policy(vec![rule(
            &[EXACT_ORIGIN],
            &["GET", "POST", "PUT"],
            &["x-request-*"],
            &["ETag", "X-Response-Id"],
            Some(90),
        )]),
    )
    .await;
    harness
        .seed_plain_object("object.txt", OBJECT_CID, b"focused CORS body")
        .await;
    harness.mount_add_failure().await;

    let mut origin = HeaderMap::new();
    origin.insert(ORIGIN, HeaderValue::from_static(EXACT_ORIGIN));
    let success = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "object.txt",
        &[],
        Vec::new(),
        origin.clone(),
        SECRET,
    )
    .await;
    assert_eq!(success.status(), StatusCode::OK);
    assert_eq!(success.headers()[ACCESS_CONTROL_ALLOW_ORIGIN], EXACT_ORIGIN);
    assert_eq!(success.headers()[ACCESS_CONTROL_ALLOW_CREDENTIALS], "true");
    assert_eq!(
        success.headers()[ACCESS_CONTROL_EXPOSE_HEADERS],
        "ETag, X-Response-Id"
    );
    assert!(!success.headers().contains_key(ACCESS_CONTROL_MAX_AGE));
    assert_eq!(
        success
            .bytes()
            .await
            .expect("read positive object")
            .as_ref(),
        b"focused CORS body"
    );

    let mut disallowed_origin = HeaderMap::new();
    disallowed_origin.insert(ORIGIN, HeaderValue::from_static(OTHER_ORIGIN));
    let unchanged = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "object.txt",
        &[],
        Vec::new(),
        disallowed_origin,
        SECRET,
    )
    .await;
    assert_eq!(unchanged.status(), StatusCode::OK);
    assert_no_cors_headers(&unchanged);

    let missing = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "missing.txt",
        &[],
        Vec::new(),
        origin.clone(),
        SECRET,
    )
    .await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_eq!(missing.headers()[ACCESS_CONTROL_ALLOW_ORIGIN], EXACT_ORIGIN);
    assert_eq!(
        missing.headers()[ACCESS_CONTROL_EXPOSE_HEADERS],
        "ETag, X-Response-Id"
    );

    let mut put_origin = HeaderMap::new();
    put_origin.insert(ORIGIN, HeaderValue::from_static(EXACT_ORIGIN));
    let inner_failure = send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "put-error.txt",
        &[],
        b"inner storage failure".to_vec(),
        put_origin,
        SECRET,
    )
    .await;
    assert_eq!(inner_failure.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        inner_failure.headers()[ACCESS_CONTROL_ALLOW_ORIGIN],
        EXACT_ORIGIN
    );

    let before_rows = side_effect_counts(&harness).await;
    let before_kubo = harness.kubo_request_count().await;
    for (query, requested_method) in [("ipfs3-import", "POST"), ("decompress-zip", "PUT")] {
        let path = format!("/{}/archive.zip?{query}", harness.bucket);
        let response = browser_request(
            &harness,
            reqwest::Method::OPTIONS,
            &path,
            preflight_headers(EXACT_ORIGIN, requested_method, Some("X-Request-Id")),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[ACCESS_CONTROL_ALLOW_ORIGIN],
            EXACT_ORIGIN
        );
    }
    let invalid_import = browser_request(
        &harness,
        reqwest::Method::OPTIONS,
        &format!("/{}/key?ipfs3-import", harness.bucket),
        preflight_headers(EXACT_ORIGIN, "POST", Some("X-Blocked")),
    )
    .await;
    assert_eq!(invalid_import.status(), StatusCode::FORBIDDEN);
    assert_eq!(side_effect_counts(&harness).await, before_rows);
    assert_eq!(harness.kubo_request_count().await, before_kubo);

    for path in ["/health", "/ready"] {
        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, HeaderValue::from_static(EXACT_ORIGIN));
        let response = browser_request(&harness, reqwest::Method::GET, path, headers).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_no_cors_headers(&response);
        assert!(!response.headers().contains_key(VARY));
    }
}
