use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use http_body_util::BodyExt as _;
use sea_orm::{ActiveModelTrait, Database, EntityTrait, PaginatorTrait, Set, TransactionTrait};

use super::request::*;
use super::*;
use crate::{
    import::{
        ImportConfig,
        downloader::{
            DownloadError, DownloadLimits, ImportResolver, ReqwestImportHttpTransport,
            SourceDownloader, StrictPublicAddressPolicy,
        },
    },
    store::{
        Store,
        entities::{import_destination, import_job, import_job_result},
    },
};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

fn cid_xml() -> String {
    format!("<IPFS3ImportRequest><CID>{CID}</CID></IPFS3ImportRequest>")
}

async fn test_state() -> Arc<AppState> {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    crate::store::bucket::create(&db, "bucket", None)
        .await
        .unwrap();
    Arc::new(AppState {
        kubo: crate::kubo::KuboClient::new("http://127.0.0.1:1".to_owned()),
        cold_kubo: None,
        store: Store::new(db),
        credentials: HashMap::new(),
        master_key: crate::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: crate::pinning::coordinator::PinningCoordinator::disabled_for_test(),
    })
}

fn coordinator(config: ImportConfig) -> Arc<ImportCoordinator> {
    let config = config.validate().unwrap();
    let downloader = SourceDownloader::production(Arc::new(config.clone()));
    ImportCoordinator::new(config, downloader)
}

async fn setup_route(enabled: bool) -> (ImportObjectRoute, Arc<AppState>) {
    let state = test_state().await;
    let coordinator = coordinator(ImportConfig {
        enabled,
        ..ImportConfig::default()
    });
    (ImportObjectRoute::new(state.clone(), coordinator), state)
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

async fn response_xml(response: S3Response<Body>) -> String {
    String::from_utf8(response.output.collect().await.unwrap().to_bytes().to_vec()).unwrap()
}

async fn submit_cid(route: &ImportObjectRoute, uri: &str) -> S3Response<Body> {
    route
        .call(request(Method::POST, uri, Body::from(cid_xml())))
        .await
        .unwrap()
}

#[tokio::test]
async fn bounded_body_accepts_exactly_64_kib_and_rejects_the_next_byte() {
    let mut at_limit = Body::from(vec![b'x'; MAX_IMPORT_XML_BYTES]);
    assert_eq!(
        collect_import_xml(&mut at_limit).await.unwrap().len(),
        MAX_IMPORT_XML_BYTES
    );
    let mut over_limit = Body::from(vec![b'x'; MAX_IMPORT_XML_BYTES + 1]);
    assert_eq!(
        collect_import_xml(&mut over_limit)
            .await
            .unwrap_err()
            .code()
            .as_str(),
        "InvalidArgument"
    );
}

#[test]
fn strict_xml_accepts_exactly_one_cid_or_url() {
    assert_eq!(
        parse_import_xml(cid_xml().as_bytes()).unwrap(),
        ParsedImportSource::Cid(CID.to_owned())
    );
    assert!(matches!(
        parse_import_xml(
            b"<IPFS3ImportRequest><URL>https://example.com/file</URL></IPFS3ImportRequest>"
        ),
        Ok(ParsedImportSource::Url(_))
    ));
    assert!(parse_import_xml(b"<IPFS3ImportRequest></IPFS3ImportRequest>").is_err());
    assert!(
        parse_import_xml(
            format!(
                "<IPFS3ImportRequest><CID>{CID}</CID><URL>https://example.com/file</URL></IPFS3ImportRequest>"
            )
            .as_bytes()
        )
        .is_err()
    );
}

#[test]
fn strict_xml_rejects_unknown_duplicate_dtd_entity_mixed_root_and_trailing_content() {
    let documents = [
        "<IPFS3ImportRequest><Unknown>x</Unknown></IPFS3ImportRequest>".to_owned(),
        format!("<IPFS3ImportRequest><CID>{CID}</CID><CID>{CID}</CID></IPFS3ImportRequest>"),
        format!("<!DOCTYPE x><IPFS3ImportRequest><CID>{CID}</CID></IPFS3ImportRequest>"),
        "<IPFS3ImportRequest><URL>https://example.com/a&amp;b</URL></IPFS3ImportRequest>"
            .to_owned(),
        format!("<IPFS3ImportRequest>mixed<CID>{CID}</CID></IPFS3ImportRequest>"),
        format!("<Wrong><CID>{CID}</CID></Wrong>"),
        format!("<IPFS3ImportRequest><CID>{CID}</CID></IPFS3ImportRequest><extra/>"),
        format!("<IPFS3ImportRequest><CID attr=\"x\">{CID}</CID></IPFS3ImportRequest>"),
        format!("<IPFS3ImportRequest><![CDATA[x]]><CID>{CID}</CID></IPFS3ImportRequest>"),
    ];
    for document in documents {
        assert!(parse_import_xml(document.as_bytes()).is_err(), "{document}");
    }
}

#[test]
fn raw_empty_and_nonempty_url_userinfo_are_rejected_before_url_canonicalization() {
    for url in [
        "https://@example.com/file",
        "https://user@example.com/file",
        "https://user:password@example.com/file",
        r"https:\\@example.com/file",
    ] {
        let xml = format!("<IPFS3ImportRequest><URL>{url}</URL></IPFS3ImportRequest>");
        assert!(parse_import_xml(xml.as_bytes()).is_err());
    }
}

#[test]
fn submit_and_status_queries_enforce_cardinality_values_and_upload_combinations() {
    for uri in [
        "/bucket/key?ipfs3-import=value",
        "/bucket/key?ipfs3-import&ipfs3-import",
        "/bucket/key?ipfs3-import&decompress-zip=a&decompress-zip=b",
        "/bucket/key?ipfs3-import&uploads",
        "/bucket/key?ipfs3-import&uploadId=id",
        "/bucket/key?ipfs3-import&decompress-zip-result=true",
        "/bucket/key?ipfs3-import&max-results=10",
    ] {
        assert!(parse_submit_query(&uri.parse().unwrap()).is_err(), "{uri}");
    }
    assert!(
        parse_submit_query(
            &"/bucket/key?ipfs3-import&decompress-zip=prefix%2F"
                .parse()
                .unwrap()
        )
        .is_ok()
    );

    let job = uuid::Uuid::new_v4().to_string();
    assert!(parse_status_query(&"/bucket/key?ipfs3-import".parse().unwrap()).is_err());
    for query in [
        format!("/bucket/key?ipfs3-import={job}&ipfs3-import={job}"),
        format!("/bucket/key?ipfs3-import={job}&max-results=1&max-results=2"),
        format!("/bucket/key?ipfs3-import={job}&continuation-token=a&continuation-token=b"),
    ] {
        assert!(
            parse_status_query(&query.parse().unwrap()).is_err(),
            "{query}"
        );
    }
    let parsed = parse_status_query(
        &format!("/bucket/key?ipfs3-import={job}&max-results=5000")
            .parse()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(parsed.max_results, MAX_RESULTS);
    assert_eq!(
        parse_status_query(&format!("/bucket/key?ipfs3-import={job}").parse().unwrap())
            .unwrap()
            .max_results,
        DEFAULT_MAX_RESULTS
    );
}

#[tokio::test]
async fn submit_rejects_non_xml_content_type_and_every_sse_header_without_a_job() {
    let (route, state) = setup_route(true).await;
    let mut non_xml = request(
        Method::POST,
        "/bucket/key?ipfs3-import",
        Body::from(cid_xml()),
    );
    non_xml.headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/octet-stream"),
    );
    assert_eq!(
        route.call(non_xml).await.unwrap_err().code().as_str(),
        "InvalidArgument"
    );

    for header in [
        "x-amz-server-side-encryption",
        "x-amz-server-side-encryption-customer-algorithm",
        "x-amz-server-side-encryption-customer-key",
        "x-amz-server-side-encryption-customer-key-md5",
    ] {
        let mut sse = request(
            Method::POST,
            "/bucket/key?ipfs3-import",
            Body::from(cid_xml()),
        );
        sse.headers
            .insert(header, http::HeaderValue::from_static("secret"));
        assert_eq!(
            route.call(sse).await.unwrap_err().code().as_str(),
            "InvalidArgument"
        );
    }
    assert_eq!(
        import_job::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn disabled_feature_does_not_create_a_job() {
    let (route, state) = setup_route(false).await;
    let error = route
        .call(request(
            Method::POST,
            "/bucket/key?ipfs3-import",
            Body::from(cid_xml()),
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code().as_str(), "NotImplemented");
    assert_eq!(
        import_job::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn submit_captures_metadata_tags_and_returns_accepted_headers_and_xml() {
    let (route, state) = setup_route(true).await;
    let mut submit = request(
        Method::POST,
        "/bucket/key?ipfs3-import&decompress-zip=prefix%2F",
        Body::from(cid_xml()),
    );
    submit.headers.insert(
        "x-ipfs3-object-content-type",
        http::HeaderValue::from_static("application/octet-stream"),
    );
    submit.headers.insert(
        "x-ipfs3-client-token",
        http::HeaderValue::from_static("client-token"),
    );
    submit.headers.insert(
        "x-amz-tagging",
        http::HeaderValue::from_static("project=alpha%26beta"),
    );
    submit
        .headers
        .insert("x-amz-meta-owner", http::HeaderValue::from_static("alice"));

    let response = route.call(submit).await.unwrap();
    assert_eq!(response.status, Some(StatusCode::ACCEPTED));
    assert_eq!(
        response.headers[http::header::CONTENT_TYPE],
        "application/xml"
    );
    let job_id = response.headers["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        response.headers[http::header::LOCATION],
        format!("/bucket/key?ipfs3-import={job_id}")
    );
    let xml = response_xml(response).await;
    assert!(xml.contains(&format!("<JobId>{job_id}</JobId>")));
    assert!(xml.contains("<State>queued</State><Phase>queued</Phase>"));

    let job = import_job::Entity::find_by_id(job_id)
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        job.object_content_type.as_deref(),
        Some("application/octet-stream")
    );
    assert_eq!(job.client_token.as_deref(), Some("client-token"));
    assert_eq!(job.decompress_prefix.as_deref(), Some("prefix/"));
    assert_eq!(job.metadata_json, r#"{"owner":"alice"}"#);
    assert_eq!(job.tags_json, r#"[{"key":"project","value":"alpha&beta"}]"#);
}

#[tokio::test]
async fn idempotent_replay_returns_original_job_and_mismatch_is_stable_conflict() {
    let (route, state) = setup_route(true).await;
    let tokenized = || {
        let mut request = request(
            Method::POST,
            "/bucket/key?ipfs3-import",
            Body::from(cid_xml()),
        );
        request.headers.insert(
            "x-ipfs3-client-token",
            http::HeaderValue::from_static("same-token"),
        );
        request
    };
    let first = route.call(tokenized()).await.unwrap();
    let first_id = first.headers["x-ipfs3-import-job-id"].clone();
    let replay = route.call(tokenized()).await.unwrap();
    assert_eq!(replay.headers["x-ipfs3-import-job-id"], first_id);
    assert_eq!(
        import_job::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        1
    );

    let mut mismatch = tokenized();
    mismatch.headers.insert(
        "x-amz-meta-version",
        http::HeaderValue::from_static("different"),
    );
    let error = route.call(mismatch).await.unwrap_err();
    assert_eq!(error.code().as_str(), "IdempotentParameterMismatch");
    assert_eq!(error.status_code(), Some(StatusCode::CONFLICT));
}

struct ControlledResolver {
    calls: Arc<AtomicUsize>,
    fail: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl ImportResolver for ControlledResolver {
    async fn resolve(&self, _host: &str, port: u16) -> Result<Vec<SocketAddr>, DownloadError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            return Err(DownloadError::Dns);
        }
        Ok(vec![SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
            port,
        )])
    }
}

async fn url_route() -> (
    ImportObjectRoute,
    Arc<AppState>,
    Arc<AtomicUsize>,
    Arc<AtomicBool>,
) {
    let state = test_state().await;
    let config = ImportConfig {
        allowed_https_origins: vec!["https://example.com".to_owned()],
        ..ImportConfig::default()
    }
    .validate()
    .unwrap();
    let resolves = Arc::new(AtomicUsize::new(0));
    let resolver_fail = Arc::new(AtomicBool::new(false));
    let limits = DownloadLimits {
        connect_timeout: std::time::Duration::from_secs(1),
        idle_timeout: std::time::Duration::from_secs(1),
        max_bytes: 1,
    };
    let downloader = SourceDownloader::with_components(
        Arc::new(config.clone()),
        Arc::new(ControlledResolver {
            calls: resolves.clone(),
            fail: resolver_fail.clone(),
        }),
        Arc::new(StrictPublicAddressPolicy),
        Arc::new(ReqwestImportHttpTransport::new(limits, Vec::new())),
    );
    let coordinator = ImportCoordinator::new(config, downloader);
    (
        ImportObjectRoute::new(state.clone(), coordinator),
        state,
        resolves,
        resolver_fail,
    )
}

#[tokio::test]
async fn access_precedes_body_url_authorization_and_database_work() {
    let (route, state, resolves, _resolver_fail) = url_route().await;
    let mut unauthorized = request(
        Method::POST,
        "/bucket/key?ipfs3-import",
        Body::from(
            "<IPFS3ImportRequest><URL>https://example.com/secret</URL></IPFS3ImportRequest>"
                .to_owned(),
        ),
    );
    unauthorized.credentials = None;

    let error = route.call(unauthorized).await.unwrap_err();
    assert_eq!(error.code().as_str(), "AccessDenied");
    assert_eq!(resolves.load(Ordering::SeqCst), 0);
    assert_eq!(
        import_job::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn url_is_authorized_before_atomic_submit_and_never_appears_in_api_xml() {
    let (route, state, resolves, _resolver_fail) = url_route().await;
    let response = route
        .call(request(
            Method::POST,
            "/bucket/key?ipfs3-import",
            Body::from(
                "<IPFS3ImportRequest><URL>https://example.com/secret-token</URL></IPFS3ImportRequest>"
                    .to_owned(),
            ),
        ))
        .await
        .unwrap();
    let job_id = response.headers["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(resolves.load(Ordering::SeqCst), 1);
    assert!(!response_xml(response).await.contains("example.com"));

    let status = route
        .call(request(
            Method::GET,
            &format!("/bucket/key?ipfs3-import={job_id}"),
            Body::empty(),
        ))
        .await
        .unwrap();
    assert!(!response_xml(status).await.contains("example.com"));
    assert_eq!(
        import_job::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn identical_tokenized_url_replay_bypasses_later_resolver_failure() {
    let (route, state, resolves, resolver_fail) = url_route().await;
    let tokenized_url_request = || {
        let mut request = request(
            Method::POST,
            "/bucket/key?ipfs3-import",
            Body::from(
                "<IPFS3ImportRequest><URL>https://example.com/object</URL></IPFS3ImportRequest>"
                    .to_owned(),
            ),
        );
        request.headers.insert(
            "x-ipfs3-client-token",
            http::HeaderValue::from_static("url-replay-token"),
        );
        request
    };

    let created = route.call(tokenized_url_request()).await.unwrap();
    let original_job_id = created.headers["x-ipfs3-import-job-id"].clone();
    assert_eq!(resolves.load(Ordering::SeqCst), 1);
    resolver_fail.store(true, Ordering::SeqCst);

    let replay = route.call(tokenized_url_request()).await.unwrap();
    assert_eq!(replay.headers["x-ipfs3-import-job-id"], original_job_id);
    assert_eq!(resolves.load(Ordering::SeqCst), 1);
    assert_eq!(
        import_job::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn deleted_bucket_precedes_token_replay_or_conflict_without_resolver_or_ownership_work() {
    let (route, state, resolves, resolver_fail) = url_route().await;
    let tokenized_url_request = || {
        let mut request = request(
            Method::POST,
            "/bucket/key?ipfs3-import",
            Body::from(
                "<IPFS3ImportRequest><URL>https://example.com/object</URL></IPFS3ImportRequest>"
                    .to_owned(),
            ),
        );
        request.headers.insert(
            "x-ipfs3-client-token",
            http::HeaderValue::from_static("deleted-bucket-token"),
        );
        request
    };

    let created = route.call(tokenized_url_request()).await.unwrap();
    let job_id = created.headers["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(resolves.load(Ordering::SeqCst), 1);

    let submitted = import_job::Entity::find_by_id(&job_id)
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    let superseded_at = submitted.updated_at + chrono::Duration::seconds(1);
    let superseded = state
        .store
        .db()
        .transaction(|txn| {
            Box::pin(async move {
                crate::store::import::ownership::lock_bucket_for_ownership(txn, "bucket").await?;
                crate::store::import::ownership::supersede_bucket(txn, "bucket", superseded_at)
                    .await
            })
        })
        .await
        .unwrap();
    assert_eq!(superseded, 1);
    assert_eq!(
        import_job::Entity::find_by_id(&job_id)
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "superseded"
    );

    crate::store::bucket::delete(state.store.db(), "bucket")
        .await
        .unwrap();
    let retained_before = import_job::Entity::find_by_id(&job_id)
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    let destinations_before = import_destination::Entity::find()
        .count(state.store.db())
        .await
        .unwrap();
    resolver_fail.store(true, Ordering::SeqCst);

    let replay_error = route.call(tokenized_url_request()).await.unwrap_err();
    assert_eq!(replay_error.code().as_str(), "NoSuchBucket");
    assert_eq!(replay_error.status_code(), Some(StatusCode::NOT_FOUND));

    let mut mismatch = tokenized_url_request();
    mismatch.headers.insert(
        "x-amz-meta-version",
        http::HeaderValue::from_static("different"),
    );
    let mismatch_error = route.call(mismatch).await.unwrap_err();
    assert_eq!(mismatch_error.code().as_str(), "NoSuchBucket");
    assert_eq!(mismatch_error.status_code(), Some(StatusCode::NOT_FOUND));
    assert_eq!(resolves.load(Ordering::SeqCst), 1);

    let retained_after = import_job::Entity::find_by_id(&job_id)
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained_after, retained_before);
    assert_eq!(
        import_destination::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        destinations_before
    );
    assert_eq!(
        import_job::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn token_fingerprint_conflict_precedes_url_authorization_and_preserves_ownership() {
    let (route, state, resolves, resolver_fail) = url_route().await;
    let mut cid_request = request(
        Method::POST,
        "/bucket/key?ipfs3-import",
        Body::from(cid_xml()),
    );
    cid_request.headers.insert(
        "x-ipfs3-client-token",
        http::HeaderValue::from_static("cross-source-token"),
    );
    route.call(cid_request).await.unwrap();
    let before = import_destination::Entity::find_by_id(("bucket".to_owned(), "key".to_owned()))
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    resolver_fail.store(true, Ordering::SeqCst);

    let mut conflicting_url = request(
        Method::POST,
        "/bucket/key?ipfs3-import",
        Body::from(
            "<IPFS3ImportRequest><URL>https://example.com/unreachable</URL></IPFS3ImportRequest>"
                .to_owned(),
        ),
    );
    conflicting_url.headers.insert(
        "x-ipfs3-client-token",
        http::HeaderValue::from_static("cross-source-token"),
    );
    let error = route.call(conflicting_url).await.unwrap_err();

    assert_eq!(error.code().as_str(), "IdempotentParameterMismatch");
    assert_eq!(resolves.load(Ordering::SeqCst), 0);
    assert_eq!(
        import_job::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        1
    );
    let after = import_destination::Entity::find_by_id(("bucket".to_owned(), "key".to_owned()))
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.generation, before.generation);
    assert_eq!(after.owner_job_id, before.owner_job_id);
}

#[tokio::test]
async fn status_lookup_is_path_bound() {
    let (route, _state) = setup_route(true).await;
    let unknown = route
        .call(request(
            Method::GET,
            &format!("/bucket/key?ipfs3-import={}", uuid::Uuid::new_v4()),
            Body::empty(),
        ))
        .await
        .unwrap_err();
    assert_eq!(unknown.code().as_str(), "NoSuchImportJob");

    let accepted = submit_cid(&route, "/bucket/key?ipfs3-import").await;
    let job_id = accepted.headers["x-ipfs3-import-job-id"].to_str().unwrap();
    let mismatch = route
        .call(request(
            Method::GET,
            &format!("/bucket/other?ipfs3-import={job_id}"),
            Body::empty(),
        ))
        .await
        .unwrap_err();
    assert_eq!(mismatch.code().as_str(), "NoSuchImportJob");
}

#[tokio::test]
async fn completed_decompression_results_page_and_continue_with_job_bound_token() {
    let (route, state) = setup_route(true).await;
    let accepted = submit_cid(
        &route,
        "/bucket/archive.zip?ipfs3-import&decompress-zip=prefix%2F",
    )
    .await;
    let job_id = accepted.headers["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let job = import_job::Entity::find_by_id(&job_id)
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    let mut active: import_job::ActiveModel = job.into();
    active.state = Set("completed".to_owned());
    active.final_cid = Set(Some(CID.to_owned()));
    active.logical_size = Set(Some(12));
    active.update(state.store.db()).await.unwrap();
    for sequence in 0..3 {
        import_job_result::Entity::insert(import_job_result::ActiveModel {
            job_id: Set(job_id.clone()),
            sequence: Set(sequence),
            key: Set(format!("prefix/key-{sequence}")),
            cid: Set(Some(format!("bafy-{sequence}"))),
            size: Set(Some(sequence)),
            error_code: Set(None),
            error_message: Set(None),
        })
        .exec(state.store.db())
        .await
        .unwrap();
    }

    let first = route
        .call(request(
            Method::GET,
            &format!("/bucket/archive.zip?ipfs3-import={job_id}&max-results=2"),
            Body::empty(),
        ))
        .await
        .unwrap();
    let first = response_xml(first).await;
    assert!(first.contains("prefix/key-0"));
    assert!(first.contains("prefix/key-1"));
    assert!(!first.contains("prefix/key-2"));
    assert!(first.contains("<IsTruncated>true</IsTruncated>"));
    let token = first
        .split_once("<NextContinuationToken>")
        .unwrap()
        .1
        .split_once("</NextContinuationToken>")
        .unwrap()
        .0;

    let second = route
        .call(request(
            Method::GET,
            &format!(
                "/bucket/archive.zip?ipfs3-import={job_id}&max-results=2&continuation-token={token}"
            ),
            Body::empty(),
        ))
        .await
        .unwrap();
    let second = response_xml(second).await;
    assert!(!second.contains("prefix/key-1"));
    assert!(second.contains("prefix/key-2"));
    assert!(second.contains("<IsTruncated>false</IsTruncated>"));
    assert!(!second.contains("NextContinuationToken"));
}

#[test]
fn continuation_tokens_are_job_bound_and_reject_tampering() {
    let job = uuid::Uuid::new_v4().to_string();
    let other = uuid::Uuid::new_v4().to_string();
    let token = encode_continuation_token(&job, 99);
    assert_eq!(decode_continuation_token(&job, &token).unwrap(), 99);
    assert!(decode_continuation_token(&other, &token).is_err());
    assert!(decode_continuation_token(&job, "not-base64!").is_err());
}
