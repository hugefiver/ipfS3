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
    s3::sigv4,
    store::{
        Store,
        entities::{import_destination, import_job, import_job_result},
    },
};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

#[tokio::test]
async fn signed_zip_submission_captures_root_option_and_replay_preserves_it() {
    let (route, state) = setup_route(true).await;
    let uri = "/bucket/archive.zip?ipfs3-import&decompress-zip=out/";
    let mut first = request(Method::POST, uri, Body::from(cid_xml()));
    first
        .headers
        .insert("x-ipfs3-client-token", "root-capture".parse().unwrap());
    first
        .headers
        .insert("x-amz-tagging", "ipfs-s3%3Azip-root=false".parse().unwrap());
    assert_eq!(
        route.call(first).await.unwrap().status,
        Some(StatusCode::ACCEPTED)
    );
    let stored = import_job::Entity::find()
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.root_capture_json.as_deref(),
        Some("{\"tagged\":false}")
    );

    let mut replay = request(Method::POST, uri, Body::from(cid_xml()));
    replay
        .headers
        .insert("x-ipfs3-client-token", "root-capture".parse().unwrap());
    replay
        .headers
        .insert("x-amz-tagging", "ipfs-s3%3Azip-root=false".parse().unwrap());
    assert_eq!(
        route.call(replay).await.unwrap().status,
        Some(StatusCode::ACCEPTED)
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
async fn configured_root_default_is_not_a_signed_request_fingerprint_field() {
    let (route, state) = setup_route(true).await;
    let uri = "/bucket/archive.zip?ipfs3-import&decompress-zip=out/";
    let mut first = request(Method::POST, uri, Body::from(cid_xml()));
    first
        .headers
        .insert("x-ipfs3-client-token", "default-change".parse().unwrap());
    assert_eq!(
        route.call(first).await.unwrap().status,
        Some(StatusCode::ACCEPTED)
    );
    let initial = import_job::Entity::find()
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        initial.root_capture_json.as_deref(),
        Some("{\"configured\":true}")
    );
    let changed = ImportObjectRoute::with_root_default(
        state.clone(),
        coordinator(ImportConfig {
            enabled: true,
            ..ImportConfig::default()
        }),
        false,
    );
    let mut replay = request(Method::POST, uri, Body::from(cid_xml()));
    replay
        .headers
        .insert("x-ipfs3-client-token", "default-change".parse().unwrap());
    let response = changed.call(replay).await.unwrap();
    assert_eq!(response.headers["x-ipfs3-import-job-id"], initial.id);
    assert_eq!(
        import_job::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        1
    );
}

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

#[tokio::test]
async fn same_client_token_cannot_replay_another_authenticated_principals_decision() {
    let (route, state) = setup_route(true).await;
    let mut first = request(
        Method::POST,
        "/bucket/key?ipfs3-import",
        Body::from(cid_xml()),
    );
    first
        .headers
        .insert("x-ipfs3-client-token", "shared-token".parse().unwrap());
    route.call(first).await.unwrap();
    let mut other = request(
        Method::POST,
        "/bucket/key?ipfs3-import",
        Body::from(cid_xml()),
    );
    other
        .headers
        .insert("x-ipfs3-client-token", "shared-token".parse().unwrap());
    other.credentials.as_mut().unwrap().access_key = "other".into();
    assert_eq!(
        route.call(other).await.unwrap_err().code().as_str(),
        "IdempotentParameterMismatch"
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
async fn warn_submission_persists_skipped_decision_and_replay_keeps_its_warning() {
    use crate::{
        config::{OptionalPinControlMode, PinningConfig},
        pinning::{
            config::ValidatedPinningConfig,
            decision::{DecisionEffect, ExtensionDecision},
        },
    };

    let base = test_state().await;
    let config = ValidatedPinningConfig::from_raw(&PinningConfig::default(), |_| None).unwrap();
    let state = Arc::new(AppState {
        kubo: base.kubo.clone(),
        cold_kubo: None,
        store: base.store.clone(),
        credentials: HashMap::new(),
        master_key: crate::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: crate::pinning::coordinator::PinningCoordinator::build_with_kubo_and_mode(
            config,
            None,
            OptionalPinControlMode::Warn,
        )
        .unwrap(),
    });
    let route = ImportObjectRoute::new(state.clone(), coordinator(ImportConfig::default()));
    let submit = || {
        let mut req = request(
            Method::POST,
            "/bucket/key?ipfs3-import",
            Body::from(cid_xml()),
        );
        req.headers
            .insert("x-ipfs3-client-token", "token".parse().unwrap());
        req.headers.insert(
            "x-amz-tagging",
            "ipfs-s3%3Apin=true&secret=do-not-leak".parse().unwrap(),
        );
        req
    };
    let first = route.call(submit()).await.unwrap();
    assert_eq!(first.status, Some(StatusCode::ACCEPTED));
    assert_eq!(
        first.headers["x-ipfs3-pin-warning"],
        "pin-policy-unavailable"
    );
    assert!(!response_xml(first).await.contains("do-not-leak"));
    let replay = route.call(submit()).await.unwrap();
    assert_eq!(
        replay.headers["x-ipfs3-pin-warning"],
        "pin-policy-unavailable"
    );
    let id = replay.headers["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let stored = import_job::Entity::find_by_id(&id)
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    let decision: ExtensionDecision =
        serde_json::from_str(stored.pin_decision_json.as_deref().unwrap()).unwrap();
    assert_eq!(decision.effect, DecisionEffect::Skipped);
    assert_eq!(decision.origin.request_id, id);
    assert_eq!(decision.origin.principal_id, "test");
    assert!(decision.effective_intents.is_empty());
    let status = route
        .call(request(
            Method::GET,
            &format!("/bucket/key?ipfs3-import={id}"),
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(status.headers["x-ipfs3-pin-decision"], "skipped");
    assert_eq!(
        status.headers["x-ipfs3-pin-warning"],
        "pin-policy-unavailable"
    );
    let xml = response_xml(status).await;
    assert!(!xml.contains("do-not-leak"));
    assert!(!xml.contains("Warning"));
}

#[tokio::test]
async fn signed_import_restarts_worker_with_new_policy_without_replaying_skipped_pin() {
    use crate::{
        config::{OptionalPinControlMode, PinningConfig, PolicyConfig, ProviderConfig},
        pinning::config::ValidatedPinningConfig,
        store::entities::{pin_job, pin_lease, pin_provider_usage, remote_pin},
    };
    use sea_orm::ConnectOptions;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("restart.sqlite");
    let database_url = format!(
        "sqlite://{}?mode=rwc",
        database_path.display().to_string().replace('\\', "/")
    );
    let kubo = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/routing/findprovs"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("{\"Type\":4,\"Responses\":[{\"ID\":\"provider-a\"}]}\n"),
        )
        .expect(1)
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("progress", "true"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{CID}\"]}}\n")),
        )
        .expect(1)
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello"))
        .expect(1)
        .mount(&kubo)
        .await;

    let make_state = |db, pinning| {
        Arc::new(AppState {
            kubo: crate::kubo::KuboClient::new(kubo.uri()),
            cold_kubo: None,
            store: Store::new(db),
            credentials: HashMap::from([("test".into(), s3s::auth::SecretKey::from("test"))]),
            master_key: crate::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
            pinning,
        })
    };
    let mut options = ConnectOptions::new(database_url.clone());
    options.max_connections(2);
    let db = Database::connect(options).await.unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    crate::store::bucket::create(&db, "bucket", None)
        .await
        .unwrap();
    let empty = ValidatedPinningConfig::from_raw(&PinningConfig::default(), |_| None).unwrap();
    let original = make_state(
        db,
        crate::pinning::coordinator::PinningCoordinator::build_with_kubo_and_mode(
            empty,
            None,
            OptionalPinControlMode::Warn,
        )
        .unwrap(),
    );
    let imports = coordinator(ImportConfig {
        poll_interval_ms: 10,
        ..ImportConfig::default()
    });
    let (endpoint, server) = signed_import_server(original.clone(), imports.clone()).await;
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        "application/xml".parse().unwrap(),
    );
    headers.insert("x-ipfs3-client-token", "restart-token".parse().unwrap());
    headers.insert(
        "x-amz-tagging",
        "ipfs-s3%3Apin=true&private=do-not-leak".parse().unwrap(),
    );
    let submit = sigv4::send_sigv4(
        reqwest::Method::POST,
        &endpoint,
        "bucket",
        "key",
        &[("ipfs3-import", "")],
        cid_xml().into_bytes(),
        headers.clone(),
        "test",
    )
    .await;
    assert_eq!(submit.status(), reqwest::StatusCode::ACCEPTED);
    assert_eq!(
        submit.headers()["x-ipfs3-pin-warning"],
        "pin-policy-unavailable"
    );
    assert!(!submit.headers().contains_key("x-ipfs3-pin-decision"));
    let id = submit.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert!(!submit.text().await.unwrap().contains("do-not-leak"));
    server.abort();
    drop(original);

    let mut raw = PinningConfig::default();
    raw.providers.push(ProviderConfig {
        name: "new-provider".into(),
        kind: "noop".into(),
        token_env: None,
        endpoint: None,
        api: None,
        strategy: None,
        upload_endpoint: None,
        enabled: true,
        priority: 1,
        max_bytes: 1000,
        max_pins: 100,
        requests_per_second: None,
    });
    raw.policies.push(PolicyConfig {
        bucket: "bucket".into(),
        prefix: String::new(),
        trigger: "request".into(),
        provider_mode: "one".into(),
        providers: vec!["new-provider".into()],
        default_duration: "1h".into(),
        max_duration: "24h".into(),
        allow_decompressed: false,
    });
    let pinning = crate::pinning::coordinator::PinningCoordinator::build_with_kubo_and_mode(
        ValidatedPinningConfig::from_raw(&raw, |_| None).unwrap(),
        None,
        OptionalPinControlMode::Warn,
    )
    .unwrap();
    let restarted = make_state(Database::connect(database_url).await.unwrap(), pinning);
    let (endpoint, server) = signed_import_server(restarted.clone(), imports.clone()).await;
    let replay = sigv4::send_sigv4(
        reqwest::Method::POST,
        &endpoint,
        "bucket",
        "key",
        &[("ipfs3-import", "")],
        cid_xml().into_bytes(),
        headers,
        "test",
    )
    .await;
    assert_eq!(replay.status(), reqwest::StatusCode::ACCEPTED);
    assert_eq!(replay.headers()["x-ipfs3-import-job-id"], id);
    assert_eq!(
        replay.headers()["x-ipfs3-pin-warning"],
        "pin-policy-unavailable"
    );

    let worker = imports.start(
        restarted.clone(),
        tokio_util::sync::CancellationToken::new(),
    );
    let completed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let row = import_job::Entity::find_by_id(&id)
                .one(restarted.store.db())
                .await
                .unwrap()
                .unwrap();
            if row.state == "completed" {
                break row;
            }
            assert_ne!(
                row.state, "failed",
                "worker failed: {:?}",
                row.failure_message
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    worker.shutdown(std::time::Duration::from_secs(1)).await;
    assert_eq!(completed.final_cid.as_deref(), Some(CID));
    let status = sigv4::send_sigv4(
        reqwest::Method::GET,
        &endpoint,
        "bucket",
        "key",
        &[("ipfs3-import", &id)],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(status.status(), reqwest::StatusCode::OK);
    assert_eq!(status.headers()["x-ipfs3-pin-decision"], "skipped");
    assert_eq!(
        status.headers()["x-ipfs3-pin-warning"],
        "pin-policy-unavailable"
    );
    let xml = status.text().await.unwrap();
    assert!(xml.contains("<State>completed</State>"));
    assert!(!xml.contains("Warning"));
    assert!(!xml.contains("do-not-leak"));
    for count in [
        pin_lease::Entity::find()
            .count(restarted.store.db())
            .await
            .unwrap(),
        pin_job::Entity::find()
            .count(restarted.store.db())
            .await
            .unwrap(),
        pin_provider_usage::Entity::find()
            .count(restarted.store.db())
            .await
            .unwrap(),
        remote_pin::Entity::find()
            .count(restarted.store.db())
            .await
            .unwrap(),
    ] {
        assert_eq!(count, 0);
    }
    server.abort();
}

async fn signed_import_server(
    state: Arc<AppState>,
    imports: Arc<ImportCoordinator>,
) -> (String, tokio::task::JoinHandle<()>) {
    signed_import_server_with_root_default(state, imports, true).await
}

async fn signed_import_server_with_root_default(
    state: Arc<AppState>,
    imports: Arc<ImportCoordinator>,
    root_default: bool,
) -> (String, tokio::task::JoinHandle<()>) {
    use axum::error_handling::HandleError;
    use s3s::service::S3ServiceBuilder;
    let mut builder = S3ServiceBuilder::new(crate::s3::handler::S3Impl::new(state.clone()));
    builder.set_auth(crate::auth::GatewayAuth::new(state.clone()));
    builder.set_route(crate::s3::route::gateway::GatewayRoute::with_root_default(
        state,
        imports,
        root_default,
    ));
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
    (endpoint, server)
}

#[tokio::test]
async fn signed_zip_root_is_visible_only_after_restarted_worker_commits_receipt_and_versions() {
    use crate::import::{
        decompress::tests::{mount_verified_directory, zip},
        pipeline::ImportExecutionObserver,
    };
    use sea_orm::ConnectOptions;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };
    struct StopBeforePublication(tokio::sync::Notify);
    #[async_trait::async_trait]
    impl ImportExecutionObserver for StopBeforePublication {
        async fn before_publication(&self, _: &str) {
            self.0.notify_one();
            std::future::pending::<()>().await;
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let database_url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("stage4-root.sqlite")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let kubo = MockServer::start().await;
    let archive = zip(&[("file.txt", b"hello")]);
    let root =
        cid::Cid::new_v1(0x70, CID.parse::<cid::Cid>().unwrap().hash().to_owned()).to_string();
    Mock::given(method("POST"))
        .and(path("/api/v0/routing/findprovs"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("{\"Type\":4,\"Responses\":[{\"ID\":\"provider-a\"}]}\n"),
        )
        .expect(2)
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(archive))
        .expect(4)
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("progress", "true"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{CID}\"]}}\n")),
        )
        .expect(2)
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Hash\":\"{CID}\",\"Size\":\"5\"}}\n")),
        )
        .expect(2)
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("arg", CID))
        .respond_with(ResponseTemplate::new(200))
        .expect(2)
        .mount(&kubo)
        .await;
    mount_verified_directory(&kubo, CID, &root).await;

    let mut options = ConnectOptions::new(database_url.clone());
    options.max_connections(2);
    let db = Database::connect(options).await.unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    crate::store::bucket::create(&db, "bucket", None)
        .await
        .unwrap();
    let make_state = |db| {
        Arc::new(AppState {
            kubo: crate::kubo::KuboClient::new(kubo.uri()),
            cold_kubo: None,
            store: Store::new(db),
            credentials: HashMap::from([("test".into(), s3s::auth::SecretKey::from("test"))]),
            master_key: crate::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
            pinning: crate::pinning::coordinator::PinningCoordinator::disabled_for_test(),
        })
    };
    let original = make_state(db);
    let import_config = ImportConfig {
        poll_interval_ms: 10,
        lease_duration_secs: 2,
        ..ImportConfig::default()
    };
    let validated = import_config.clone().validate().unwrap();
    let downloader = SourceDownloader::production(Arc::new(validated.clone()));
    let pause = Arc::new(StopBeforePublication(tokio::sync::Notify::new()));
    let first_imports = ImportCoordinator::new_with_observer(validated, downloader, pause.clone());
    let (endpoint, server) = signed_import_server(original.clone(), first_imports.clone()).await;
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        "application/xml".parse().unwrap(),
    );
    headers.insert("x-ipfs3-client-token", "stage4-restart".parse().unwrap());
    headers.insert("x-amz-tagging", "ipfs-s3%3Azip-root=true".parse().unwrap());
    let submit = sigv4::send_sigv4(
        reqwest::Method::POST,
        &endpoint,
        "bucket",
        "archive.zip",
        &[("ipfs3-import", ""), ("decompress-zip", "out/")],
        cid_xml().into_bytes(),
        headers.clone(),
        "test",
    )
    .await;
    assert_eq!(submit.status(), reqwest::StatusCode::ACCEPTED);
    assert!(!submit.headers().contains_key("x-ipfs-s3-zip-root-cid"));
    let id = submit.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert!(!submit.text().await.unwrap().contains("ZipRoot"));
    let before = sigv4::send_sigv4(
        reqwest::Method::GET,
        &endpoint,
        "bucket",
        "archive.zip",
        &[("ipfs3-import", &id)],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert!(!before.text().await.unwrap().contains("ZipRoot"));
    let first_worker =
        first_imports.start(original.clone(), tokio_util::sync::CancellationToken::new());
    tokio::time::timeout(std::time::Duration::from_secs(5), pause.0.notified())
        .await
        .unwrap();
    first_worker
        .shutdown(std::time::Duration::from_secs(2))
        .await;
    assert!(
        crate::store::zip::snapshot(original.store.db(), &id)
            .await
            .unwrap()
            .is_none()
    );
    server.abort();
    drop(original);

    let restarted = make_state(Database::connect(database_url).await.unwrap());
    let imports = coordinator(import_config);
    let (endpoint, server) =
        signed_import_server_with_root_default(restarted.clone(), imports.clone(), false).await;
    let replay = sigv4::send_sigv4(
        reqwest::Method::POST,
        &endpoint,
        "bucket",
        "archive.zip",
        &[("ipfs3-import", ""), ("decompress-zip", "out/")],
        cid_xml().into_bytes(),
        headers,
        "test",
    )
    .await;
    assert_eq!(replay.status(), reqwest::StatusCode::ACCEPTED);
    assert_eq!(replay.headers()["x-ipfs3-import-job-id"], id);
    let stored = import_job::Entity::find_by_id(&id)
        .one(restarted.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.root_capture_json.as_deref(),
        Some("{\"tagged\":true}")
    );

    tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
    let worker = imports.start(
        restarted.clone(),
        tokio_util::sync::CancellationToken::new(),
    );
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        loop {
            let job = import_job::Entity::find_by_id(&id)
                .one(restarted.store.db())
                .await
                .unwrap()
                .unwrap();
            if job.state == "completed" {
                break;
            }
            assert_ne!(job.state, "failed", "worker error {:?}", job.failure_code);
            tokio::time::sleep(std::time::Duration::from_millis(15)).await;
        }
    })
    .await
    .unwrap();
    worker.shutdown(std::time::Duration::from_secs(2)).await;
    let status = sigv4::send_sigv4(
        reqwest::Method::GET,
        &endpoint,
        "bucket",
        "archive.zip",
        &[("ipfs3-import", &id)],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(status.status(), reqwest::StatusCode::OK);
    let xml = status.text().await.unwrap();
    assert!(
        xml.contains(&format!(
            "<ZipRoot><Status>complete</Status><CID>{root}</CID></ZipRoot>"
        )),
        "{xml}"
    );
    assert!(
        xml.contains(&format!("<Artifact><CID>{CID}</CID>")),
        "{xml}"
    );
    let snapshot = crate::store::zip::snapshot(restarted.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.batch.root_cid.as_deref(), Some(root.as_str()));
    assert_eq!(snapshot.references[0].state, "adopted");

    let mut signed_false = HeaderMap::new();
    signed_false.insert(
        http::header::CONTENT_TYPE,
        "application/xml".parse().unwrap(),
    );
    signed_false.insert("x-amz-tagging", "ipfs-s3%3Azip-root=false".parse().unwrap());
    let false_submit = sigv4::send_sigv4(
        reqwest::Method::POST,
        &endpoint,
        "bucket",
        "another.zip",
        &[("ipfs3-import", ""), ("decompress-zip", "out/")],
        cid_xml().into_bytes(),
        signed_false,
        "test",
    )
    .await;
    assert_eq!(false_submit.status(), reqwest::StatusCode::ACCEPTED);
    let false_id = false_submit.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap();
    assert_eq!(
        import_job::Entity::find_by_id(false_id)
            .one(restarted.store.db())
            .await
            .unwrap()
            .unwrap()
            .root_capture_json
            .as_deref(),
        Some("{\"tagged\":false}")
    );
    server.abort();
}

#[tokio::test]
async fn signed_zip_config_off_persists_disabled_and_never_builds_a_root() {
    use crate::import::decompress::tests::zip;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };
    let kubo = MockServer::start().await;
    let archive = zip(&[("file.txt", b"hello")]);
    Mock::given(method("POST"))
        .and(path("/api/v0/routing/findprovs"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("{\"Type\":4,\"Responses\":[{\"ID\":\"provider-a\"}]}\n"),
        )
        .expect(1)
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("progress", "true"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{CID}\"]}}\n")),
        )
        .expect(1)
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(archive))
        .expect(2)
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string("{\"Hash\":\"QmEntry\",\"Size\":\"5\"}\n"),
        )
        .expect(1)
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&kubo)
        .await;
    let db = Database::connect("sqlite::memory:").await.unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    crate::store::bucket::create(&db, "bucket", None)
        .await
        .unwrap();
    let state = Arc::new(AppState {
        kubo: crate::kubo::KuboClient::new(kubo.uri()),
        cold_kubo: None,
        store: Store::new(db),
        credentials: HashMap::from([("test".into(), s3s::auth::SecretKey::from("test"))]),
        master_key: crate::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: crate::pinning::coordinator::PinningCoordinator::disabled_for_test(),
    });
    let imports = coordinator(ImportConfig {
        poll_interval_ms: 10,
        ..ImportConfig::default()
    });
    let (endpoint, server) =
        signed_import_server_with_root_default(state.clone(), imports.clone(), false).await;
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        "application/xml".parse().unwrap(),
    );
    let accepted = sigv4::send_sigv4(
        reqwest::Method::POST,
        &endpoint,
        "bucket",
        "archive.zip",
        &[("ipfs3-import", ""), ("decompress-zip", "out/")],
        cid_xml().into_bytes(),
        headers,
        "test",
    )
    .await;
    assert_eq!(accepted.status(), reqwest::StatusCode::ACCEPTED);
    let id = accepted.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        import_job::Entity::find_by_id(&id)
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap()
            .root_capture_json
            .as_deref(),
        Some("{\"configured\":false}")
    );
    let worker = imports.start(state.clone(), tokio_util::sync::CancellationToken::new());
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        loop {
            let job = import_job::Entity::find_by_id(&id)
                .one(state.store.db())
                .await
                .unwrap()
                .unwrap();
            if job.state == "completed" {
                break;
            }
            assert_ne!(job.state, "failed", "worker error {:?}", job.failure_code);
            tokio::time::sleep(std::time::Duration::from_millis(15)).await;
        }
    })
    .await
    .unwrap();
    worker.shutdown(std::time::Duration::from_secs(2)).await;
    let response = sigv4::send_sigv4(
        reqwest::Method::GET,
        &endpoint,
        "bucket",
        "archive.zip",
        &[("ipfs3-import", &id)],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert!(
        response
            .text()
            .await
            .unwrap()
            .contains("<ZipRoot><Status>disabled</Status></ZipRoot>")
    );
    assert!(
        kubo.received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.url.path() != "/api/v0/dag/put")
    );
    server.abort();
}

#[tokio::test]
async fn signed_zip_worker_reports_failed_partial_and_zero_file_roots_without_losing_results() {
    use crate::import::decompress::tests::{mount_verified_directory, zip};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };
    for (case, names, expected) in [
        ("failed", vec!["file.txt"], "failed"),
        ("partial", vec!["file.txt", "bad.txt"], "partial"),
        ("zero", vec!["bad.txt"], "empty"),
    ] {
        let kubo = MockServer::start().await;
        let entries = names
            .iter()
            .map(|name| (*name, b"hello".as_slice()))
            .collect::<Vec<_>>();
        let archive = zip(&entries);
        Mock::given(method("POST"))
            .and(path("/api/v0/routing/findprovs"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("{\"Type\":4,\"Responses\":[{\"ID\":\"provider-a\"}]}\n"),
            )
            .expect(1)
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .and(query_param("progress", "true"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{CID}\"]}}\n")),
            )
            .expect(1)
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(archive))
            .expect(2)
            .mount(&kubo)
            .await;
        let count = Arc::new(AtomicUsize::new(0));
        let calls = count.clone();
        let case_owned = case.to_owned();
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(move |_: &wiremock::Request| {
                if case_owned == "zero" || calls.fetch_add(1, Ordering::SeqCst) > 0 {
                    ResponseTemplate::new(500)
                } else {
                    let leaf = if case_owned == "failed" {
                        "QmEntry"
                    } else {
                        CID
                    };
                    ResponseTemplate::new(200)
                        .set_body_string(format!("{{\"Hash\":\"{leaf}\",\"Size\":\"5\"}}\n"))
                }
            })
            .expect(names.len() as u64)
            .mount(&kubo)
            .await;
        if case != "zero" {
            Mock::given(method("POST"))
                .and(path("/api/v0/pin/add"))
                .and(query_param(
                    "arg",
                    if case == "failed" { "QmEntry" } else { CID },
                ))
                .respond_with(ResponseTemplate::new(200))
                .expect(1)
                .mount(&kubo)
                .await;
        }
        let root =
            cid::Cid::new_v1(0x70, CID.parse::<cid::Cid>().unwrap().hash().to_owned()).to_string();
        if case == "partial" {
            mount_verified_directory(&kubo, CID, &root).await;
        }
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "bucket", None)
            .await
            .unwrap();
        let state = Arc::new(AppState {
            kubo: crate::kubo::KuboClient::new(kubo.uri()),
            cold_kubo: None,
            store: Store::new(db),
            credentials: HashMap::from([("test".into(), s3s::auth::SecretKey::from("test"))]),
            master_key: crate::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
            pinning: crate::pinning::coordinator::PinningCoordinator::disabled_for_test(),
        });
        let imports = coordinator(ImportConfig {
            poll_interval_ms: 10,
            ..ImportConfig::default()
        });
        let (endpoint, server) = signed_import_server(state.clone(), imports.clone()).await;
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            "application/xml".parse().unwrap(),
        );
        headers.insert("x-amz-tagging", "ipfs-s3%3Azip-root=true".parse().unwrap());
        let submit = sigv4::send_sigv4(
            reqwest::Method::POST,
            &endpoint,
            "bucket",
            "archive.zip",
            &[("ipfs3-import", ""), ("decompress-zip", "out/")],
            cid_xml().into_bytes(),
            headers,
            "test",
        )
        .await;
        assert_eq!(submit.status(), reqwest::StatusCode::ACCEPTED, "{case}");
        let id = submit.headers()["x-ipfs3-import-job-id"]
            .to_str()
            .unwrap()
            .to_owned();
        let worker = imports.start(state.clone(), tokio_util::sync::CancellationToken::new());
        tokio::time::timeout(std::time::Duration::from_secs(8), async {
            loop {
                let job = import_job::Entity::find_by_id(&id)
                    .one(state.store.db())
                    .await
                    .unwrap()
                    .unwrap();
                if job.state == "completed" {
                    break;
                }
                assert_ne!(job.state, "failed", "{case}: {:?}", job.failure_code);
                tokio::time::sleep(std::time::Duration::from_millis(15)).await;
            }
        })
        .await
        .unwrap();
        worker.shutdown(std::time::Duration::from_secs(2)).await;
        let status = sigv4::send_sigv4(
            reqwest::Method::GET,
            &endpoint,
            "bucket",
            "archive.zip",
            &[("ipfs3-import", &id)],
            Vec::new(),
            HeaderMap::new(),
            "test",
        )
        .await;
        assert_eq!(status.status(), reqwest::StatusCode::OK, "{case}");
        let xml = status.text().await.unwrap();
        assert!(
            xml.contains(&format!("<ZipRoot><Status>{expected}</Status>")),
            "{case}: {xml}"
        );
        let snapshot = crate::store::zip::snapshot(state.store.db(), &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.batch.root_status, expected);
        if case == "partial" {
            assert!(xml.contains(&format!("<CID>{root}</CID></ZipRoot>")));
            assert!(xml.contains("<Status>failure</Status>"));
        } else {
            assert!(snapshot.batch.root_cid.is_none());
            if case == "failed" {
                assert!(xml.contains("<ErrorCode>invalid_manifest</ErrorCode>"));
            }
        }
        assert_eq!(
            snapshot.entries.len(),
            if case == "partial" { 2 } else { 1 }
        );
        server.abort();
    }
}

#[tokio::test]
async fn signed_zip_stale_worker_cannot_build_or_publish_after_target_supersession() {
    use crate::{
        import::{SupersedeReason, decompress::tests::zip, pipeline::ImportExecutionObserver},
        store::import::ownership,
    };
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };
    struct SupersedeOnPublication(Arc<AppState>);
    #[async_trait::async_trait]
    impl ImportExecutionObserver for SupersedeOnPublication {
        async fn before_publication(&self, _: &str) {
            ownership::admit_content_mutation(
                self.0.store.db(),
                "bucket",
                "out/file.txt",
                None,
                SupersedeReason::PutObject,
                chrono::Utc::now(),
            )
            .await
            .unwrap();
        }
    }
    let kubo = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/routing/findprovs"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("{\"Type\":4,\"Responses\":[{\"ID\":\"provider-a\"}]}\n"),
        )
        .expect(1)
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("progress", "true"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{CID}\"]}}\n")),
        )
        .expect(1)
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(zip(&[("file.txt", b"hello")])))
        .expect(2)
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string("{\"Hash\":\"QmEntry\",\"Size\":\"5\"}\n"),
        )
        .expect(1)
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&kubo)
        .await;
    let db = Database::connect("sqlite::memory:").await.unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    crate::store::bucket::create(&db, "bucket", None)
        .await
        .unwrap();
    let state = Arc::new(AppState {
        kubo: crate::kubo::KuboClient::new(kubo.uri()),
        cold_kubo: None,
        store: Store::new(db),
        credentials: HashMap::from([("test".into(), s3s::auth::SecretKey::from("test"))]),
        master_key: crate::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: crate::pinning::coordinator::PinningCoordinator::disabled_for_test(),
    });
    let config = ImportConfig {
        poll_interval_ms: 10,
        ..ImportConfig::default()
    }
    .validate()
    .unwrap();
    let downloader = SourceDownloader::production(Arc::new(config.clone()));
    let imports = ImportCoordinator::new_with_observer(
        config,
        downloader,
        Arc::new(SupersedeOnPublication(state.clone())),
    );
    let (endpoint, server) = signed_import_server(state.clone(), imports.clone()).await;
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        "application/xml".parse().unwrap(),
    );
    headers.insert("x-amz-tagging", "ipfs-s3%3Azip-root=true".parse().unwrap());
    let accepted = sigv4::send_sigv4(
        reqwest::Method::POST,
        &endpoint,
        "bucket",
        "archive.zip",
        &[("ipfs3-import", ""), ("decompress-zip", "out/")],
        cid_xml().into_bytes(),
        headers,
        "test",
    )
    .await;
    assert_eq!(accepted.status(), reqwest::StatusCode::ACCEPTED);
    let id = accepted.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let worker = imports.start(state.clone(), tokio_util::sync::CancellationToken::new());
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        loop {
            let job = import_job::Entity::find_by_id(&id)
                .one(state.store.db())
                .await
                .unwrap()
                .unwrap();
            if job.state == "superseded" {
                break;
            }
            assert_ne!(
                job.state, "failed",
                "unexpected worker failure {:?}",
                job.failure_code
            );
            tokio::time::sleep(std::time::Duration::from_millis(15)).await;
        }
    })
    .await
    .unwrap();
    worker.shutdown(std::time::Duration::from_secs(2)).await;
    assert!(
        crate::store::zip::snapshot(state.store.db(), &id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        crate::store::entities::object::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    let status = sigv4::send_sigv4(
        reqwest::Method::GET,
        &endpoint,
        "bucket",
        "archive.zip",
        &[("ipfs3-import", &id)],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(status.status(), reqwest::StatusCode::OK);
    assert!(!status.text().await.unwrap().contains("<ZipRoot>"));
    assert!(
        kubo.received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.url.path() != "/api/v0/dag/put")
    );
    server.abort();
}

struct ControlledResolver {
    calls: Arc<AtomicUsize>,
    fail: Arc<AtomicBool>,
}

struct BlockingSubmissionResolver {
    started: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl ImportResolver for BlockingSubmissionResolver {
    async fn resolve(&self, _host: &str, _port: u16) -> Result<Vec<SocketAddr>, DownloadError> {
        self.started.notify_one();
        std::future::pending().await
    }
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
async fn submission_dns_timeout_rejects_before_database_work() {
    let state = test_state().await;
    let config = ImportConfig {
        allowed_https_origins: vec!["https://example.com".to_owned()],
        connect_timeout_secs: 1,
        ..ImportConfig::default()
    }
    .validate()
    .unwrap();
    let resolver = Arc::new(BlockingSubmissionResolver {
        started: tokio::sync::Notify::new(),
    });
    let limits = DownloadLimits {
        connect_timeout: std::time::Duration::from_secs(1),
        idle_timeout: std::time::Duration::from_secs(1),
        max_bytes: 1,
    };
    let downloader = SourceDownloader::with_components(
        Arc::new(config.clone()),
        resolver.clone(),
        Arc::new(StrictPublicAddressPolicy),
        Arc::new(ReqwestImportHttpTransport::new(limits, Vec::new())),
    );
    let route = ImportObjectRoute::new(state.clone(), ImportCoordinator::new(config, downloader));
    tokio::time::pause();
    let submit = tokio::spawn(async move {
        route
            .call(request(
                Method::POST,
                "/bucket/key?ipfs3-import",
                Body::from(
                    "<IPFS3ImportRequest><URL>https://example.com/object</URL></IPFS3ImportRequest>"
                        .to_owned(),
                ),
            ))
            .await
    });

    resolver.started.notified().await;
    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    tokio::time::timeout(std::time::Duration::from_secs(1), submit)
        .await
        .expect("submission must finish after the DNS deadline")
        .unwrap()
        .expect_err("timed-out submission DNS must be rejected");
    tokio::time::resume();
    assert_eq!(
        import_job::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
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
