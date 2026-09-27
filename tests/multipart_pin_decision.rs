//! End-to-end signed HTTP coverage for admission-time MPU/ZIP pin decisions.
use std::{collections::HashMap, sync::Arc};

use axum::error_handling::HandleError;
use http::{HeaderMap, HeaderValue};
use ipfs_s3_gateway::{
    auth::GatewayAuth,
    config::{OptionalPinControlMode, PinningConfig, PolicyConfig, ProviderConfig},
    crypto::key::MasterKey,
    kubo::KuboClient,
    pinning::{
        config::ValidatedPinningConfig, coordinator::PinningCoordinator, decision::DecisionEffect,
    },
    s3::{handler::S3Impl, route::decompress_zip::DecompressZipRoute},
    state::AppState,
    store::{
        self, Store,
        entities::{multipart_upload, pin_job, pin_lease},
    },
};
use s3s::service::S3ServiceBuilder;
use sea_orm::{Database, EntityTrait, PaginatorTrait};
use sea_orm_migration::{MigrationTrait, SchemaManager};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[path = "../src/store/migrations/m20260920_000004_multipart_pin_decision.rs"]
mod pending_migration;
#[path = "support/sigv4.rs"]
#[allow(dead_code)]
mod sigv4;

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

fn zip_bytes() -> Vec<u8> {
    // Stored entry "a" containing one zero byte; CRC32(\0) = d202ef8d.
    let mut zip = Vec::new();
    zip.extend_from_slice(&0x04034b50u32.to_le_bytes());
    zip.extend_from_slice(&20u16.to_le_bytes());
    zip.extend_from_slice(&[0; 8]); // flags, compression, DOS time and date
    zip.extend_from_slice(&0xd202ef8du32.to_le_bytes());
    zip.extend_from_slice(&1u32.to_le_bytes());
    zip.extend_from_slice(&1u32.to_le_bytes());
    zip.extend_from_slice(&1u16.to_le_bytes());
    zip.extend_from_slice(&0u16.to_le_bytes());
    zip.extend_from_slice(b"a\0");
    let central = zip.len() as u32;
    zip.extend_from_slice(&0x02014b50u32.to_le_bytes());
    zip.extend_from_slice(&20u16.to_le_bytes());
    zip.extend_from_slice(&20u16.to_le_bytes());
    zip.extend_from_slice(&[0; 8]);
    zip.extend_from_slice(&0xd202ef8du32.to_le_bytes());
    zip.extend_from_slice(&1u32.to_le_bytes());
    zip.extend_from_slice(&1u32.to_le_bytes());
    zip.extend_from_slice(&1u16.to_le_bytes());
    zip.extend_from_slice(&0u16.to_le_bytes());
    zip.extend_from_slice(&[0; 14]); // comment length, disk, attrs, local header offset
    zip.extend_from_slice(b"a");
    let central_size = zip.len() as u32 - central;
    zip.extend_from_slice(&0x06054b50u32.to_le_bytes());
    zip.extend_from_slice(&[0; 4]);
    zip.extend_from_slice(&1u16.to_le_bytes());
    zip.extend_from_slice(&1u16.to_le_bytes());
    zip.extend_from_slice(&central_size.to_le_bytes());
    zip.extend_from_slice(&central.to_le_bytes());
    zip.extend_from_slice(&0u16.to_le_bytes());
    zip
}

async fn kubo() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Hash\":\"{CID}\",\"Size\":\"1\"}}\n")),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(zip_bytes()))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[]}"))
        .mount(&server)
        .await;
    server
}

fn pinning(raw: PinningConfig, mode: OptionalPinControlMode) -> Arc<PinningCoordinator> {
    let mut config: ipfs_s3_gateway::config::Config = toml::from_str("").unwrap();
    config.pinning = raw;
    config.pinning_control.unavailable = mode;
    let validated = ValidatedPinningConfig::from_config(&config, |_| None).unwrap();
    PinningCoordinator::build_with_kubo_and_mode(validated, None, mode).unwrap()
}

fn available_policy(trigger: &str) -> PinningConfig {
    PinningConfig {
        providers: vec![ProviderConfig {
            name: "new-provider".into(),
            kind: "noop".into(),
            token_env: None,
            endpoint: None,
            api: None,
            strategy: None,
            upload_endpoint: None,
            enabled: true,
            priority: 1,
            max_bytes: 10_000,
            max_pins: 100,
            requests_per_second: None,
        }],
        policies: vec![PolicyConfig {
            bucket: "bucket".into(),
            prefix: String::new(),
            trigger: trigger.into(),
            provider_mode: "one".into(),
            providers: vec!["new-provider".into()],
            default_duration: "1h".into(),
            max_duration: "24h".into(),
            allow_decompressed: true,
        }],
        ..PinningConfig::default()
    }
}

async fn state(kubo: &MockServer, mode: OptionalPinControlMode) -> Arc<AppState> {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let manager = SchemaManager::new(&db);
    if !manager
        .has_column("multipart_uploads", "pin_decision_json")
        .await
        .unwrap()
    {
        pending_migration::Migration.up(&manager).await.unwrap();
    }
    store::bucket::create(&db, "bucket", None).await.unwrap();
    Arc::new(AppState {
        kubo: KuboClient::new(kubo.uri()),
        cold_kubo: None,
        store: Store::new(db),
        credentials: HashMap::from([("test".into(), s3s::auth::SecretKey::from("test"))]),
        master_key: MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: pinning(PinningConfig::default(), mode),
    })
}

async fn serve(state: Arc<AppState>) -> (String, tokio::task::JoinHandle<()>) {
    state
        .pinning
        .register_identities(&state.store)
        .await
        .unwrap();
    let mut builder = S3ServiceBuilder::new(S3Impl::new(state.clone()));
    builder.set_auth(GatewayAuth::new(state.clone()));
    builder.set_route(DecompressZipRoute::new(state));
    let app = axum::Router::new().fallback_service(HandleError::new(
        builder.build(),
        |_: s3s::HttpError| async {
            http::Response::builder()
                .status(500)
                .body(s3s::Body::empty())
                .unwrap()
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (endpoint, server)
}

fn tags() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-tagging",
        HeaderValue::from_static("ipfs-s3%3Apin=true&private=do-not-leak"),
    );
    headers
}

fn upload_id(body: &str) -> String {
    body.split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_owned()
}

async fn part(endpoint: &str, key: &str, upload_id: &str) {
    let part = sigv4::send_sigv4(
        reqwest::Method::PUT,
        endpoint,
        "bucket",
        key,
        &[("partNumber", "1"), ("uploadId", upload_id)],
        zip_bytes(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(
        part.status(),
        reqwest::StatusCode::OK,
        "{}",
        part.text().await.unwrap()
    );
}

async fn complete(endpoint: &str, key: &str, upload_id: &str) -> reqwest::Response {
    let body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{CID}\"</ETag></Part></CompleteMultipartUpload>"
    );
    sigv4::send_sigv4(
        reqwest::Method::POST,
        endpoint,
        "bucket",
        key,
        &[("uploadId", upload_id)],
        body.into_bytes(),
        HeaderMap::new(),
        "test",
    )
    .await
}

async fn no_remote_work(state: &AppState) {
    assert_eq!(
        pin_lease::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        pin_job::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn signed_multipart_warn_capture_replays_after_provider_appears_and_abort_has_no_work() {
    let kubo = kubo().await;
    let first = state(&kubo, OptionalPinControlMode::Warn).await;
    let (endpoint, server) = serve(first.clone()).await;
    let created = sigv4::send_sigv4(
        reqwest::Method::POST,
        &endpoint,
        "bucket",
        "normal",
        &[("uploads", "")],
        Vec::new(),
        tags(),
        "test",
    )
    .await;
    assert_eq!(created.status(), reqwest::StatusCode::OK);
    assert_eq!(
        created.headers()["x-ipfs3-pin-warning"],
        "pin-policy-unavailable"
    );
    let id = upload_id(&created.text().await.unwrap());
    let row = store::multipart::get_upload(first.store.db(), &id)
        .await
        .unwrap();
    let decision = store::multipart::decision_from_upload(&row)
        .unwrap()
        .unwrap();
    assert_eq!(decision.effect, DecisionEffect::Skipped);
    assert_eq!(decision.origin.principal_id, "test");
    assert_eq!(decision.origin.request_id, id);
    assert!(
        !row.pin_decision_json
            .unwrap()
            .to_string()
            .contains("do-not-leak")
    );

    let abort = sigv4::send_sigv4(
        reqwest::Method::DELETE,
        &endpoint,
        "bucket",
        "normal",
        &[("uploadId", &id)],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(abort.status(), reqwest::StatusCode::NO_CONTENT);
    no_remote_work(&first).await;

    let created = sigv4::send_sigv4(
        reqwest::Method::POST,
        &endpoint,
        "bucket",
        "normal",
        &[("uploads", "")],
        Vec::new(),
        tags(),
        "test",
    )
    .await;
    let id = upload_id(&created.text().await.unwrap());
    part(&endpoint, "normal", &id).await;
    server.abort();
    let restarted = Arc::new(AppState {
        kubo: first.kubo.clone(),
        cold_kubo: None,
        store: first.store.clone(),
        credentials: first.credentials.clone(),
        master_key: MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: pinning(available_policy("request"), OptionalPinControlMode::Strict),
    });
    let (endpoint, server) = serve(restarted.clone()).await;
    let done = complete(&endpoint, "normal", &id).await;
    assert_eq!(done.status(), reqwest::StatusCode::OK);
    assert_eq!(
        done.headers()["x-ipfs3-pin-warning"],
        "pin-policy-unavailable"
    );
    assert!(done.headers()["etag"].to_str().unwrap().contains(CID));
    let xml = done.text().await.unwrap();
    assert!(xml.contains(CID));
    assert!(!xml.contains("do-not-leak"));
    no_remote_work(&restarted).await;
    server.abort();
}

#[tokio::test]
async fn signed_direct_zip_and_multipart_zip_warn_without_changing_response_shapes() {
    let kubo = kubo().await;
    let state = state(&kubo, OptionalPinControlMode::Warn).await;
    let (endpoint, server) = serve(state.clone()).await;
    let direct = sigv4::send_sigv4(
        reqwest::Method::PUT,
        &endpoint,
        "bucket",
        "archive.zip",
        &[("decompress-zip", "out/")],
        zip_bytes(),
        tags(),
        "test",
    )
    .await;
    assert_eq!(direct.status(), reqwest::StatusCode::OK);
    assert_eq!(
        direct.headers()["x-ipfs3-pin-warning"],
        "pin-policy-unavailable"
    );
    assert!(direct.text().await.unwrap().contains(CID));

    let created = sigv4::send_sigv4(
        reqwest::Method::POST,
        &endpoint,
        "bucket",
        "multipart.zip",
        &[("uploads", ""), ("decompress-zip", "mp/")],
        Vec::new(),
        tags(),
        "test",
    )
    .await;
    assert_eq!(created.status(), reqwest::StatusCode::OK);
    assert_eq!(
        created.headers()["x-ipfs3-pin-warning"],
        "pin-policy-unavailable"
    );
    let id = upload_id(&created.text().await.unwrap());
    part(&endpoint, "multipart.zip", &id).await;
    let done = complete(&endpoint, "multipart.zip", &id).await;
    assert_eq!(done.status(), reqwest::StatusCode::OK);
    assert_eq!(
        done.headers()["x-ipfs3-pin-warning"],
        "pin-policy-unavailable"
    );
    assert!(done.text().await.unwrap().contains(CID));
    no_remote_work(&state).await;
    server.abort();
}

#[tokio::test]
async fn signed_malformed_controls_fail_even_in_warn_mode_and_strict_stays_strict() {
    let kubo = kubo().await;
    let state = state(&kubo, OptionalPinControlMode::Warn).await;
    let (endpoint, server) = serve(state.clone()).await;
    let mut malformed = HeaderMap::new();
    malformed.insert(
        "x-amz-tagging",
        HeaderValue::from_static("ipfs-s3%3Apin=TRUE"),
    );
    for (method, key, query, body) in [
        (
            reqwest::Method::POST,
            "bad",
            vec![("uploads", "")],
            Vec::new(),
        ),
        (
            reqwest::Method::PUT,
            "bad.zip",
            vec![("decompress-zip", "out/")],
            zip_bytes(),
        ),
    ] {
        let response = sigv4::send_sigv4(
            method,
            &endpoint,
            "bucket",
            key,
            &query,
            body,
            malformed.clone(),
            "test",
        )
        .await;
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    }
    assert_eq!(
        multipart_upload::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert!(
        !kubo
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|request| { request.url.path() == "/api/v0/add" })
    );
    server.abort();
    let strict = Arc::new(AppState {
        kubo: state.kubo.clone(),
        cold_kubo: None,
        store: state.store.clone(),
        credentials: state.credentials.clone(),
        master_key: MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: pinning(PinningConfig::default(), OptionalPinControlMode::Strict),
    });
    let (endpoint, server) = serve(strict).await;
    let response = sigv4::send_sigv4(
        reqwest::Method::POST,
        &endpoint,
        "bucket",
        "strict",
        &[("uploads", "")],
        Vec::new(),
        tags(),
        "test",
    )
    .await;
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    server.abort();
}

#[tokio::test]
async fn old_uncaptured_raw_pin_tags_cannot_gain_authority_under_new_policy() {
    let kubo = kubo().await;
    let first = state(&kubo, OptionalPinControlMode::Warn).await;
    let raw = vec![ipfs_s3_gateway::pinning::tags::ObjectTag::new(
        "ipfs-s3:pin",
        "true",
    )];
    store::multipart::create_upload(
        first.store.db(),
        "old-upload",
        "old-object",
        "bucket",
        "legacy",
        "none",
        None,
        None,
        None,
        None,
        &raw,
        None,
        true,
    )
    .await
    .unwrap();
    let updated = Arc::new(AppState {
        kubo: first.kubo.clone(),
        cold_kubo: None,
        store: first.store.clone(),
        credentials: first.credentials.clone(),
        master_key: MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: pinning(available_policy("request"), OptionalPinControlMode::Strict),
    });
    let (endpoint, server) = serve(updated.clone()).await;
    part(&endpoint, "legacy", "old-upload").await;
    let done = complete(&endpoint, "legacy", "old-upload").await;
    assert_eq!(done.status(), reqwest::StatusCode::OK);
    assert!(done.headers().get("x-ipfs3-pin-warning").is_none());
    assert!(done.text().await.unwrap().contains(CID));
    no_remote_work(&updated).await;
    server.abort();
}

#[tokio::test]
async fn warn_never_drops_automatic_policy_when_provider_is_unavailable() {
    let kubo = kubo().await;
    let first = state(&kubo, OptionalPinControlMode::Warn).await;
    let mut raw = available_policy("always");
    let mut disabled = raw.providers[0].clone();
    disabled.name = "disabled-provider".into();
    disabled.enabled = false;
    disabled.priority = 2;
    raw.providers.push(disabled);
    raw.policies[0].provider_mode = "all".into();
    raw.policies[0].providers.push("disabled-provider".into());
    let updated = Arc::new(AppState {
        kubo: first.kubo.clone(),
        cold_kubo: None,
        store: first.store.clone(),
        credentials: first.credentials.clone(),
        master_key: MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: pinning(raw, OptionalPinControlMode::Warn),
    });
    let (endpoint, server) = serve(updated.clone()).await;
    let created = sigv4::send_sigv4(
        reqwest::Method::POST,
        &endpoint,
        "bucket",
        "auto",
        &[("uploads", "")],
        Vec::new(),
        tags(),
        "test",
    )
    .await;
    assert_eq!(created.status(), reqwest::StatusCode::OK);
    assert_eq!(
        created.headers()["x-ipfs3-pin-warning"],
        "pin-provider-unavailable"
    );
    let id = upload_id(&created.text().await.unwrap());
    let decision = store::multipart::decision_from_upload(
        &store::multipart::get_upload(updated.store.db(), &id)
            .await
            .unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(decision.effect, DecisionEffect::Skipped);
    assert_eq!(decision.effective_intents.len(), 1);
    assert_eq!(
        decision.effective_intents[0].source,
        ipfs_s3_gateway::pinning::policy::LeaseSource::Automatic
    );
    part(&endpoint, "auto", &id).await;
    let done = complete(&endpoint, "auto", &id).await;
    assert_eq!(done.status(), reqwest::StatusCode::BAD_REQUEST);
    let xml = done.text().await.unwrap();
    assert!(
        xml.contains("all-provider intent includes a disabled or retired provider"),
        "{xml}"
    );
    assert!(
        store::multipart::get_upload(updated.store.db(), &id)
            .await
            .is_ok()
    );
    no_remote_work(&updated).await;
    server.abort();
}

#[tokio::test]
async fn accepted_multipart_rejects_changed_provider_revision_without_reinterpreting_tags() {
    let kubo = kubo().await;
    let first = state(&kubo, OptionalPinControlMode::Warn).await;
    let admitting = Arc::new(AppState {
        kubo: first.kubo.clone(),
        cold_kubo: None,
        store: first.store.clone(),
        credentials: first.credentials.clone(),
        master_key: MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: pinning(available_policy("request"), OptionalPinControlMode::Warn),
    });
    let (endpoint, server) = serve(admitting.clone()).await;
    let created = sigv4::send_sigv4(
        reqwest::Method::POST,
        &endpoint,
        "bucket",
        "captured",
        &[("uploads", "")],
        Vec::new(),
        tags(),
        "test",
    )
    .await;
    assert_eq!(created.status(), reqwest::StatusCode::OK);
    assert!(created.headers().get("x-ipfs3-pin-warning").is_none());
    let id = upload_id(&created.text().await.unwrap());
    let snapshot = store::multipart::decision_from_upload(
        &store::multipart::get_upload(first.store.db(), &id)
            .await
            .unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(snapshot.effect, DecisionEffect::Accepted);
    part(&endpoint, "captured", &id).await;
    server.abort();

    let (endpoint, server) = serve(first.clone()).await;
    let rejected = complete(&endpoint, "captured", &id).await;
    assert_eq!(rejected.status(), reqwest::StatusCode::BAD_REQUEST);
    assert!(
        rejected
            .text()
            .await
            .unwrap()
            .contains("captured pinning configuration revision is unavailable")
    );
    assert!(
        store::multipart::get_upload(first.store.db(), &id)
            .await
            .is_ok()
    );
    no_remote_work(&first).await;
    server.abort();
}
