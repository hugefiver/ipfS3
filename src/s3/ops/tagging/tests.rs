mod refs;
mod status;

use super::*;
use crate::{
    config::{PinningConfig, PolicyConfig, ProviderConfig},
    pinning::{config::ValidatedPinningConfig, coordinator::PinningCoordinator},
    s3::sigv4,
    store::{Store, entities::object_version, pinning::tags as stored_tags},
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, Database, DatabaseConnection, EntityTrait, QueryFilter,
    TransactionTrait,
};
use std::{collections::HashMap, sync::Arc};
async fn assert_no_refs(db: &DatabaseConnection) {
    let counts = db.query_one(sea_orm::Statement::from_string(db.get_database_backend(),
        "SELECT (SELECT COUNT(*) FROM pin_refs) AS refs, (SELECT COUNT(*) FROM pin_resources) AS resources"))
        .await.unwrap().unwrap();
    assert_eq!(counts.try_get::<i64>("", "refs").unwrap(), 0);
    assert_eq!(counts.try_get::<i64>("", "resources").unwrap(), 0);
}

const BUCKET: &str = "bucket";
const KEY: &str = "key";
const OBJECT_ID: &str = "object-1";

struct Fixture {
    state: Arc<AppState>,
    policy_id: String,
}

struct VersionedFixture {
    fixture: Fixture,
    historical_object_id: String,
    historical_version_id: String,
    current_object_id: String,
    current_version_id: String,
}

fn time(hour: u32) -> DateTime<Utc> {
    format!("2026-07-22T{hour:02}:00:00Z").parse().unwrap()
}
fn coordinator_fixture() -> (Arc<PinningCoordinator>, String) {
    coordinator_fixture_with_providers(&["alpha", "beta"])
}

fn coordinator_fixture_with_providers(
    provider_names: &[&str],
) -> (Arc<PinningCoordinator>, String) {
    let validated = ValidatedPinningConfig::from_raw(
        &PinningConfig {
            worker_interval: "5s".to_owned(),
            worker_concurrency: 2,
            providers: provider_names
                .iter()
                .enumerate()
                .map(|(index, name)| ProviderConfig {
                    cluster: None,
                    name: (*name).to_owned(),
                    kind: "noop".to_owned(),
                    token_env: None,
                    endpoint: None,
                    api: None,
                    strategy: None,
                    upload_endpoint: None,
                    enabled: true,
                    priority: (index + 1) as u32,
                    max_bytes: 10_000,
                    max_pins: 100,
                    requests_per_second: None,
                })
                .collect(),
            policies: vec![PolicyConfig {
                bucket: BUCKET.to_owned(),
                prefix: String::new(),
                trigger: "request".to_owned(),
                provider_mode: "all".to_owned(),
                providers: provider_names
                    .iter()
                    .map(|name| (*name).to_owned())
                    .collect(),
                default_duration: "1h".to_owned(),
                max_duration: "24h".to_owned(),
                allow_decompressed: true,
            }],
        },
        |_| None,
    )
    .unwrap();
    let policy_id = validated.policies[0].identity.clone();
    (PinningCoordinator::build(validated).unwrap(), policy_id)
}
async fn fixture() -> Fixture {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .unwrap();
    fixture_from_db(db).await
}

async fn versioned_fixture() -> VersionedFixture {
    versioned_fixture_from(fixture().await).await
}

async fn versioned_fixture_from(fixture: Fixture) -> VersionedFixture {
    let db = fixture.state.store.db();
    crate::store::bucket::set_versioning_state(
        db,
        BUCKET,
        crate::store::object_version::BucketVersioningState::Enabled,
    )
    .await
    .unwrap();
    // This fixture begins with an unversioned null slot for the same
    // internal object. Production publications use a distinct immutable
    // owner per version; discard that fixture-only duplicate index first.
    crate::store::object_version::remove_null_slot(db, BUCKET, KEY)
        .await
        .unwrap();
    let historical = crate::store::object::get_latest(db, BUCKET, KEY)
        .await
        .unwrap();
    let historical_version_id = db
        .transaction({
            let historical = historical.clone();
            move |txn| {
                Box::pin(async move {
                    crate::store::object_version::install_content_version(
                        txn,
                        crate::store::object_version::BucketVersioningState::Enabled,
                        &historical,
                        time(1),
                    )
                    .await
                })
            }
        })
        .await
        .unwrap();
    crate::store::object::upsert(
        db,
        "object-2",
        BUCKET,
        KEY,
        "bafy-object-2",
        20,
        Some("application/octet-stream"),
        "bafy-object-2",
        None,
        false,
        None,
        None,
        false,
    )
    .await
    .unwrap();
    let current = crate::store::object::get_latest(db, BUCKET, KEY)
        .await
        .unwrap();
    let current_version_id = db
        .transaction({
            let current = current.clone();
            move |txn| {
                Box::pin(async move {
                    crate::store::object_version::install_content_version(
                        txn,
                        crate::store::object_version::BucketVersioningState::Enabled,
                        &current,
                        time(2),
                    )
                    .await
                })
            }
        })
        .await
        .unwrap();

    VersionedFixture {
        fixture,
        historical_object_id: historical.id,
        historical_version_id,
        current_object_id: current.id,
        current_version_id,
    }
}

async fn install_current_marker(fixture: &Fixture) -> String {
    fixture
        .state
        .store
        .db()
        .transaction(move |txn| {
            Box::pin(async move {
                crate::store::object_version::install_delete_marker(
                    txn,
                    crate::store::object_version::BucketVersioningState::Enabled,
                    BUCKET,
                    KEY,
                    time(3),
                )
                .await
            })
        })
        .await
        .unwrap()
}

async fn fixture_from_db(db: DatabaseConnection) -> Fixture {
    fixture_from_db_and_coordinator(db, coordinator_fixture()).await
}

async fn fixture_from_db_and_coordinator(
    db: DatabaseConnection,
    (pinning, policy_id): (Arc<PinningCoordinator>, String),
) -> Fixture {
    crate::store::run_migrations(&db).await.unwrap();
    crate::store::bucket::create(&db, BUCKET, None)
        .await
        .unwrap();
    crate::store::object::upsert(
        &db,
        OBJECT_ID,
        BUCKET,
        KEY,
        "bafy-object",
        10,
        Some("application/octet-stream"),
        "bafy-object",
        None,
        false,
        None,
        None,
        false,
    )
    .await
    .unwrap();
    let object = crate::store::object::get_latest(&db, BUCKET, KEY)
        .await
        .unwrap();
    db.transaction(move |txn| {
        Box::pin(async move {
            crate::store::object_version::install_content_version(
                txn,
                BucketVersioningState::Unversioned,
                &object,
                time(1),
            )
            .await
        })
    })
    .await
    .unwrap();
    Fixture {
        state: Arc::new(AppState {
            kubo: crate::kubo::KuboClient::new("http://127.0.0.1:5001".to_owned()),
            cold_kubo: None,
            store: Store::new(db),
            credentials: HashMap::new(),
            master_key: crate::crypto::key::MasterKey::from_hex(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .unwrap(),
            pinning,
        }),
        policy_id,
    }
}
fn request<T>(input: T, method: http::Method) -> S3Request<T> {
    S3Request {
        input,
        method,
        uri: format!("/{BUCKET}/{KEY}?tagging").parse().unwrap(),
        headers: http::HeaderMap::new(),
        extensions: http::Extensions::new(),
        credentials: Some(s3s::auth::Credentials {
            access_key: "test-owner".to_owned(),
            secret_key: s3s::auth::SecretKey::from("test"),
        }),
        region: None,
        service: None,
        trailing_headers: None,
    }
}

fn dto_tags(pairs: &[(&str, &str)]) -> Tagging {
    Tagging {
        tag_set: pairs
            .iter()
            .map(|(key, value)| Tag {
                key: Some((*key).to_owned()),
                value: Some((*value).to_owned()),
            })
            .collect(),
    }
}

fn put_request(pairs: &[(&str, &str)]) -> S3Request<PutObjectTaggingInput> {
    put_version_request(pairs, None)
}

fn put_version_request(
    pairs: &[(&str, &str)],
    version_id: Option<&str>,
) -> S3Request<PutObjectTaggingInput> {
    request(
        PutObjectTaggingInput {
            bucket: BUCKET.to_owned(),
            checksum_algorithm: None,
            content_md5: None,
            expected_bucket_owner: None,
            key: KEY.to_owned(),
            request_payer: None,
            tagging: dto_tags(pairs),
            version_id: version_id.map(str::to_owned),
        },
        http::Method::PUT,
    )
}

fn get_request(key: &str) -> S3Request<GetObjectTaggingInput> {
    get_version_request(key, None)
}

fn get_version_request(key: &str, version_id: Option<&str>) -> S3Request<GetObjectTaggingInput> {
    request(
        GetObjectTaggingInput {
            bucket: BUCKET.to_owned(),
            key: key.to_owned(),
            version_id: version_id.map(str::to_owned),
            ..Default::default()
        },
        http::Method::GET,
    )
}

fn delete_request(key: &str) -> S3Request<DeleteObjectTaggingInput> {
    delete_version_request(key, None)
}

fn delete_version_request(
    key: &str,
    version_id: Option<&str>,
) -> S3Request<DeleteObjectTaggingInput> {
    request(
        DeleteObjectTaggingInput {
            bucket: BUCKET.to_owned(),
            key: key.to_owned(),
            version_id: version_id.map(str::to_owned),
            ..Default::default()
        },
        http::Method::DELETE,
    )
}

async fn seed_tags(fixture: &Fixture, pairs: &[(&str, &str)]) {
    seed_tags_for_owner(fixture, OBJECT_ID, pairs).await;
}

async fn seed_tags_for_owner(fixture: &Fixture, object_id: &str, pairs: &[(&str, &str)]) {
    let tags = pairs
        .iter()
        .map(|(key, value)| crate::pinning::tags::ObjectTag::new(*key, *value))
        .collect::<Vec<_>>();
    stored_tags::replace_object_tags(fixture.state.store.db(), object_id, &tags)
        .await
        .unwrap();
}
fn tag_pairs(tags: &[Tag]) -> Vec<(&str, &str)> {
    tags.iter()
        .map(|tag| {
            (
                tag.key.as_deref().expect("stored tag key"),
                tag.value.as_deref().expect("stored tag value"),
            )
        })
        .collect()
}

#[tokio::test]
async fn round_trip_replaces_tags_and_get_sorts_by_key() {
    let fixture = fixture().await;

    put_object_tagging_at(
        &fixture.state,
        put_request(&[("z", "last"), ("a", "first")]),
        time(2),
    )
    .await
    .unwrap();
    let output = get_object_tagging(&fixture.state, get_request(KEY))
        .await
        .unwrap()
        .output;
    assert_eq!(
        tag_pairs(&output.tag_set),
        vec![("a", "first"), ("z", "last")]
    );

    put_object_tagging_at(
        &fixture.state,
        put_request(&[("middle", "replacement")]),
        time(3),
    )
    .await
    .unwrap();
    let output = get_object_tagging(&fixture.state, get_request(KEY))
        .await
        .unwrap()
        .output;
    assert_eq!(tag_pairs(&output.tag_set), vec![("middle", "replacement")]);
}

#[tokio::test]
async fn warn_tagging_skip_is_durable_and_never_reapplied_after_provider_appears() {
    use crate::{config::OptionalPinControlMode, pinning::decision::DecisionEffect};

    let db = Database::connect("sqlite::memory:").await.unwrap();
    let validated = ValidatedPinningConfig::from_raw(&PinningConfig::default(), |_| None).unwrap();
    let coordinator =
        PinningCoordinator::build_with_kubo_and_mode(validated, None, OptionalPinControlMode::Warn)
            .unwrap();
    let fixture = fixture_from_db_and_coordinator(db, (coordinator, "unused".into())).await;
    let request = [("ipfs-s3:pin", "true"), ("owner", "private")];
    let response = put_object_tagging_at(&fixture.state, put_request(&request), time(2))
        .await
        .unwrap();
    assert_eq!(
        response.headers["x-ipfs3-pin-warning"],
        "pin-policy-unavailable"
    );
    let version = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(OBJECT_ID))
        .one(fixture.state.store.db())
        .await
        .unwrap()
        .unwrap();
    let first =
        crate::store::pinning::decision::read_for_version(fixture.state.store.db(), &version.id)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(first.effect, DecisionEffect::Skipped);
    assert_eq!(first.origin.principal_id, "test-owner");

    let available = Arc::new(AppState {
        kubo: crate::kubo::KuboClient::new("http://127.0.0.1:5001".into()),
        cold_kubo: None,
        store: Store::new(fixture.state.store.db().clone()),
        credentials: HashMap::new(),
        master_key: crate::crypto::key::MasterKey::from_hex(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .unwrap(),
        pinning: coordinator_fixture().0,
    });
    let response = put_object_tagging_at(
        &available,
        put_request(&[("ipfs-s3:pin", "true"), ("owner", "updated")]),
        time(3),
    )
    .await
    .unwrap();
    assert_eq!(
        response.headers["x-ipfs3-pin-warning"],
        "pin-policy-unavailable"
    );
    let second =
        crate::store::pinning::decision::read_for_version(available.store.db(), &version.id)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(second.effect, DecisionEffect::Skipped);
    assert_ne!(first.control_revision, second.control_revision);
    let reapply = put_object_tagging_at(
        &available,
        put_request(&[("ipfs-s3:pin", "true"), ("ipfs-s3:duration", "1h")]),
        time(4),
    )
    .await
    .unwrap_err();
    assert_eq!(reapply.code().as_str(), "InvalidArgument");
    assert_eq!(
        crate::store::pinning::decision::read_for_version(available.store.db(), &version.id)
            .await
            .unwrap()
            .unwrap()
            .control_revision,
        second.control_revision
    );
    assert_no_refs(available.store.db()).await;
}
#[tokio::test]
async fn signed_copy_legacy_control_survives_ordinary_tagging_without_manual_work() {
    use axum::error_handling::HandleError;
    use s3s::service::S3ServiceBuilder;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    let kubo = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[\"bafy-object\"]}"))
        .mount(&kubo)
        .await;
    let fixture = fixture().await;
    seed_tags(&fixture, &[("ipfs-s3:pin", "true"), ("team", "legacy")]).await;
    let state = Arc::new(AppState {
        kubo: crate::kubo::KuboClient::new(kubo.uri()),
        cold_kubo: None,
        store: Store::new(fixture.state.store.db().clone()),
        credentials: HashMap::from([("test".into(), s3s::auth::SecretKey::from("test"))]),
        master_key: crate::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: fixture.state.pinning.clone(),
    });
    let mut builder = S3ServiceBuilder::new(crate::s3::handler::S3Impl::new(state.clone()));
    builder.set_auth(crate::auth::GatewayAuth::new(state.clone()));
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

    let mut headers = http::HeaderMap::new();
    headers.insert("x-amz-copy-source", "/bucket/key".parse().unwrap());
    let copy = sigv4::send_sigv4(
        reqwest::Method::PUT,
        &endpoint,
        BUCKET,
        "dest/legacy",
        &[],
        Vec::new(),
        headers,
        "test",
    )
    .await;
    assert_eq!(
        copy.status(),
        reqwest::StatusCode::OK,
        "{}",
        copy.text().await.unwrap_or_default()
    );
    let dest = crate::store::object::get_latest(state.store.db(), BUCKET, "dest/legacy")
        .await
        .unwrap();
    let version = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(&dest.id))
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    let first = crate::store::pinning::decision::read_for_version(state.store.db(), &version.id)
        .await
        .unwrap()
        .unwrap();
    assert!(first.legacy_unknown);
    assert_eq!(first.effect, DecisionEffect::NoIntent);
    assert_eq!(first.warning, None);

    let tagging = "<Tagging><TagSet><Tag><Key>ipfs-s3:pin</Key><Value>true</Value></Tag><Tag><Key>ipfs-s3:content</Key><Value>object</Value></Tag><Tag><Key>team</Key><Value>updated</Value></Tag></TagSet></Tagging>";
    let response = sigv4::send_sigv4(
        reqwest::Method::PUT,
        &endpoint,
        BUCKET,
        "dest/legacy",
        &[("tagging", "")],
        tagging.as_bytes().to_vec(),
        http::HeaderMap::new(),
        "test",
    )
    .await;
    let status = response.status();
    let warning = response.headers().get("x-ipfs3-pin-warning").cloned();
    let body = response.text().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    assert!(warning.is_none());
    let second = crate::store::pinning::decision::read_for_version(state.store.db(), &version.id)
        .await
        .unwrap()
        .unwrap();
    assert!(second.legacy_unknown);
    assert_eq!(second.effect, DecisionEffect::NoIntent);
    assert_eq!(second.warning, None);
    assert_eq!(second.control, first.control);

    let expanded_state = Arc::new(AppState {
        kubo: crate::kubo::KuboClient::new(kubo.uri()),
        cold_kubo: None,
        store: Store::new(state.store.db().clone()),
        credentials: HashMap::new(),
        master_key: crate::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: coordinator_fixture_with_providers(&["alpha", "beta", "gamma"]).0,
    });
    let mut req = put_request(&[("ipfs-s3:pin", "true"), ("team", "expanded")]);
    req.input.key = "dest/legacy".into();
    let response = put_object_tagging_at(&expanded_state, req, time(2))
        .await
        .unwrap();
    assert!(!response.headers.contains_key("x-ipfs3-pin-warning"));
    assert!(
        crate::store::pinning::decision::read_for_version(state.store.db(), &version.id)
            .await
            .unwrap()
            .unwrap()
            .legacy_unknown
    );

    let mut changed = put_request(&[("ipfs-s3:pin", "true"), ("ipfs-s3:duration", "1h")]);
    changed.input.key = "dest/legacy".into();
    assert_eq!(
        put_object_tagging_at(&expanded_state, changed, time(2))
            .await
            .unwrap_err()
            .code()
            .as_str(),
        "InvalidArgument"
    );

    let validated = ValidatedPinningConfig::from_raw(&PinningConfig::default(), |_| None).unwrap();
    let strict = PinningCoordinator::build_with_kubo_and_mode(
        validated,
        None,
        crate::config::OptionalPinControlMode::Strict,
    )
    .unwrap();
    let strict_state = Arc::new(AppState {
        kubo: crate::kubo::KuboClient::new(kubo.uri()),
        cold_kubo: None,
        store: Store::new(state.store.db().clone()),
        credentials: HashMap::new(),
        master_key: crate::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: strict,
    });
    let mut req = put_request(&[("ipfs-s3:pin", "true"), ("team", "strict")]);
    req.input.key = "dest/legacy".into();
    let response = put_object_tagging_at(&strict_state, req, time(3))
        .await
        .unwrap();
    assert!(!response.headers.contains_key("x-ipfs3-pin-warning"));
    let third = crate::store::pinning::decision::read_for_version(state.store.db(), &version.id)
        .await
        .unwrap()
        .unwrap();
    assert!(third.legacy_unknown);
    assert_eq!(third.warning, None);
    assert_eq!(third.effect, DecisionEffect::NoIntent);

    let mut changed = put_request(&[("ipfs-s3:pin", "true"), ("ipfs-s3:duration", "1h")]);
    changed.input.key = "dest/legacy".into();
    assert_eq!(
        put_object_tagging_at(&strict_state, changed, time(4))
            .await
            .unwrap_err()
            .code()
            .as_str(),
        "InvalidArgument"
    );
    assert_eq!(
        crate::store::pinning::decision::read_for_version(state.store.db(), &version.id)
            .await
            .unwrap()
            .unwrap(),
        third
    );
    assert_eq!(
        stored_tags::list_object_tags(state.store.db(), &dest.id)
            .await
            .unwrap(),
        vec![
            ObjectTag::new("ipfs-s3:pin", "true"),
            ObjectTag::new("team", "strict")
        ]
    );
    assert_no_refs(state.store.db()).await;
    server.abort();
}

#[tokio::test]
async fn legacy_raw_control_without_decision_row_cannot_be_reapplied_by_tagging() {
    let fixture = fixture().await;
    seed_tags(&fixture, &[("ipfs-s3:pin", "true"), ("team", "old")]).await;
    let version = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(OBJECT_ID))
        .one(fixture.state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert!(
        crate::store::pinning::decision::read_for_version(fixture.state.store.db(), &version.id)
            .await
            .unwrap()
            .is_none()
    );
    let response = put_object_tagging_at(
        &fixture.state,
        put_request(&[("ipfs-s3:pin", "true"), ("team", "new")]),
        time(2),
    )
    .await
    .unwrap();
    assert!(!response.headers.contains_key("x-ipfs3-pin-warning"));
    let captured =
        crate::store::pinning::decision::read_for_version(fixture.state.store.db(), &version.id)
            .await
            .unwrap()
            .unwrap();
    assert!(captured.legacy_unknown);
    assert_eq!(captured.effect, DecisionEffect::NoIntent);
    assert!(captured.effective_intents.is_empty());

    let error = put_object_tagging_at(
        &fixture.state,
        put_request(&[("ipfs-s3:pin", "true"), ("ipfs-s3:duration", "1h")]),
        time(3),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code().as_str(), "InvalidArgument");
    assert_eq!(
        crate::store::pinning::decision::read_for_version(fixture.state.store.db(), &version.id)
            .await
            .unwrap()
            .unwrap(),
        captured
    );
    assert_eq!(
        stored_tags::list_object_tags(fixture.state.store.db(), OBJECT_ID)
            .await
            .unwrap(),
        vec![
            ObjectTag::new("ipfs-s3:pin", "true"),
            ObjectTag::new("team", "new")
        ]
    );
    assert_no_refs(fixture.state.store.db()).await;
}

#[tokio::test]
async fn legacy_unknown_cancel_and_renew_controls_stay_unknown_on_ordinary_tagging() {
    for controls in [
        vec![("ipfs-s3:pin", "false")],
        vec![
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", "2026-07-22T08:00:00Z"),
        ],
    ] {
        let fixture = fixture().await;
        let mut initial = controls.clone();
        initial.push(("team", "old"));
        seed_tags(&fixture, &initial).await;
        let version = object_version::Entity::find()
            .filter(object_version::Column::ObjectId.eq(OBJECT_ID))
            .one(fixture.state.store.db())
            .await
            .unwrap()
            .unwrap();
        let mut replacement = controls;
        replacement.push(("team", "new"));
        let response = put_object_tagging_at(&fixture.state, put_request(&replacement), time(2))
            .await
            .unwrap();
        assert!(!response.headers.contains_key("x-ipfs3-pin-warning"));
        let decision = crate::store::pinning::decision::read_for_version(
            fixture.state.store.db(),
            &version.id,
        )
        .await
        .unwrap()
        .unwrap();
        assert!(decision.legacy_unknown);
        assert_eq!(decision.effect, DecisionEffect::NoIntent);
        assert_eq!(decision.warning, None);
        assert!(decision.effective_intents.is_empty());
        assert_no_refs(fixture.state.store.db()).await;
    }
}
#[tokio::test]
async fn malformed_version_ids_are_rejected_before_reads_or_mutations() {
    let fixture = fixture().await;
    seed_tags(&fixture, &[("before", "kept")]).await;

    let mut get = get_request("missing");
    get.input.version_id = Some("unsupported-get".to_owned());
    let get_result = get_object_tagging(&fixture.state, get).await;

    let mut put = put_request(&[("after", "rejected")]);
    put.input.version_id = Some("unsupported-put".to_owned());
    let put_result = put_object_tagging_at(&fixture.state, put, time(2)).await;

    let mut delete = delete_request(KEY);
    delete.input.version_id = Some("unsupported-delete".to_owned());
    let delete_result = delete_object_tagging_at(&fixture.state, delete, time(2)).await;

    for result in [
        get_result.map(|_| ()),
        put_result.map(|_| ()),
        delete_result.map(|_| ()),
    ] {
        let error = result.unwrap_err();
        assert_eq!(error.code().as_str(), "InvalidArgument");
    }
    let tags = get_object_tagging(&fixture.state, get_request(KEY))
        .await
        .unwrap()
        .output;
    assert_eq!(tag_pairs(&tags.tag_set), vec![("before", "kept")]);
}
#[tokio::test]
async fn nonexistent_keys_return_no_such_key_for_all_three_operations() {
    let fixture = fixture().await;
    let get_error = get_object_tagging(&fixture.state, get_request("missing"))
        .await
        .unwrap_err();
    let put_error = put_object_tagging_at(
        &fixture.state,
        request(
            PutObjectTaggingInput {
                bucket: BUCKET.to_owned(),
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
                key: "missing".to_owned(),
                request_payer: None,
                tagging: dto_tags(&[("team", "storage")]),
                version_id: None,
            },
            http::Method::PUT,
        ),
        time(2),
    )
    .await
    .unwrap_err();
    let delete_error = delete_object_tagging_at(&fixture.state, delete_request("missing"), time(2))
        .await
        .unwrap_err();

    for error in [get_error, put_error, delete_error] {
        assert_eq!(error.code().as_str(), "NoSuchKey");
    }
}

#[tokio::test]
async fn pin_true_without_manual_lease_is_rejected_without_replacing_tags() {
    let fixture = fixture().await;
    seed_tags(&fixture, &[("before", "kept")]).await;

    let error = put_object_tagging_at(
        &fixture.state,
        put_request(&[("ipfs-s3:pin", "true"), ("ipfs-s3:duration", "1h")]),
        time(2),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code().as_str(), "InvalidArgument");
    let output = get_object_tagging(&fixture.state, get_request(KEY))
        .await
        .unwrap()
        .output;
    assert_eq!(tag_pairs(&output.tag_set), vec![("before", "kept")]);
}
#[tokio::test]
async fn get_tagging_reads_exact_historical_owner() {
    let versions = versioned_fixture().await;
    seed_tags_for_owner(
        &versions.fixture,
        &versions.historical_object_id,
        &[("historical", "one")],
    )
    .await;
    seed_tags_for_owner(
        &versions.fixture,
        &versions.current_object_id,
        &[("current", "two")],
    )
    .await;

    let exact = get_object_tagging(
        &versions.fixture.state,
        get_version_request(KEY, Some(&versions.historical_version_id)),
    )
    .await
    .unwrap()
    .output;
    let current = get_object_tagging(&versions.fixture.state, get_request(KEY))
        .await
        .unwrap()
        .output;

    assert_eq!(tag_pairs(&exact.tag_set), vec![("historical", "one")]);
    assert_eq!(
        exact.version_id.as_deref(),
        Some(versions.historical_version_id.as_str())
    );
    assert_eq!(tag_pairs(&current.tag_set), vec![("current", "two")]);
    assert_eq!(
        current.version_id.as_deref(),
        Some(versions.current_version_id.as_str())
    );
}

#[tokio::test]
async fn put_and_delete_tagging_mutate_only_exact_internal_owner() {
    let versions = versioned_fixture().await;
    seed_tags_for_owner(
        &versions.fixture,
        &versions.historical_object_id,
        &[("historical", "before")],
    )
    .await;
    seed_tags_for_owner(
        &versions.fixture,
        &versions.current_object_id,
        &[("current", "kept")],
    )
    .await;

    let put = put_object_tagging_at(
        &versions.fixture.state,
        put_version_request(
            &[("historical", "replacement")],
            Some(&versions.historical_version_id),
        ),
        time(4),
    )
    .await
    .unwrap()
    .output;
    assert_eq!(
        put.version_id.as_deref(),
        Some(versions.historical_version_id.as_str())
    );
    let historical = get_object_tagging(
        &versions.fixture.state,
        get_version_request(KEY, Some(&versions.historical_version_id)),
    )
    .await
    .unwrap()
    .output;
    let current = get_object_tagging(&versions.fixture.state, get_request(KEY))
        .await
        .unwrap()
        .output;
    assert_eq!(
        tag_pairs(&historical.tag_set),
        vec![("historical", "replacement")]
    );
    assert_eq!(tag_pairs(&current.tag_set), vec![("current", "kept")]);

    let deleted = delete_object_tagging_at(
        &versions.fixture.state,
        delete_version_request(KEY, Some(&versions.historical_version_id)),
        time(5),
    )
    .await
    .unwrap()
    .output;
    assert_eq!(
        deleted.version_id.as_deref(),
        Some(versions.historical_version_id.as_str())
    );
    let historical = get_object_tagging(
        &versions.fixture.state,
        get_version_request(KEY, Some(&versions.historical_version_id)),
    )
    .await
    .unwrap()
    .output;
    let current = get_object_tagging(&versions.fixture.state, get_request(KEY))
        .await
        .unwrap()
        .output;
    assert!(historical.tag_set.is_empty());
    assert_eq!(tag_pairs(&current.tag_set), vec![("current", "kept")]);
}
#[tokio::test]
async fn current_tagging_marker_is_404() {
    let versions = versioned_fixture().await;
    let marker_version_id = install_current_marker(&versions.fixture).await;

    for error in [
        get_object_tagging(&versions.fixture.state, get_request(KEY))
            .await
            .expect_err("current marker GetObjectTagging must fail"),
        put_object_tagging_at(
            &versions.fixture.state,
            put_request(&[("unexpected", "mutation")]),
            time(4),
        )
        .await
        .expect_err("current marker PutObjectTagging must fail"),
        delete_object_tagging_at(&versions.fixture.state, delete_request(KEY), time(4))
            .await
            .expect_err("current marker DeleteObjectTagging must fail"),
    ] {
        assert_eq!(error.code().as_str(), "NoSuchKey");
        assert_eq!(error.status_code(), Some(http::StatusCode::NOT_FOUND));
        let headers = error.headers().expect("current marker headers");
        assert_eq!(headers["x-amz-delete-marker"], "true");
        assert_eq!(headers["x-amz-version-id"], marker_version_id);
    }
}

#[tokio::test]
async fn exact_tagging_marker_is_method_not_allowed() {
    let versions = versioned_fixture().await;
    let marker_version_id = install_current_marker(&versions.fixture).await;

    for error in [
        get_object_tagging(
            &versions.fixture.state,
            get_version_request(KEY, Some(&marker_version_id)),
        )
        .await
        .expect_err("explicit marker GetObjectTagging must fail"),
        put_object_tagging_at(
            &versions.fixture.state,
            put_version_request(&[("unexpected", "mutation")], Some(&marker_version_id)),
            time(4),
        )
        .await
        .expect_err("explicit marker PutObjectTagging must fail"),
        delete_object_tagging_at(
            &versions.fixture.state,
            delete_version_request(KEY, Some(&marker_version_id)),
            time(4),
        )
        .await
        .expect_err("explicit marker DeleteObjectTagging must fail"),
    ] {
        assert_eq!(error.code().as_str(), "MethodNotAllowed");
        assert_eq!(
            error.status_code(),
            Some(http::StatusCode::METHOD_NOT_ALLOWED)
        );
        let headers = error.headers().expect("explicit marker headers");
        assert_eq!(headers["x-amz-delete-marker"], "true");
        assert_eq!(headers["x-amz-version-id"], marker_version_id);
        assert!(headers.get(http::header::LAST_MODIFIED).is_some());
    }
}

#[tokio::test]
async fn unknown_tagging_version_is_no_such_version() {
    let versions = versioned_fixture().await;
    let unknown = "00000000-0000-0000-0000-000000000001";

    for error in [
        get_object_tagging(
            &versions.fixture.state,
            get_version_request(KEY, Some(unknown)),
        )
        .await
        .expect_err("unknown GetObjectTagging version must fail"),
        put_object_tagging_at(
            &versions.fixture.state,
            put_version_request(&[("unexpected", "mutation")], Some(unknown)),
            time(4),
        )
        .await
        .expect_err("unknown PutObjectTagging version must fail"),
        delete_object_tagging_at(
            &versions.fixture.state,
            delete_version_request(KEY, Some(unknown)),
            time(4),
        )
        .await
        .expect_err("unknown DeleteObjectTagging version must fail"),
    ] {
        assert_eq!(error.code().as_str(), "NoSuchVersion");
        assert_eq!(error.status_code(), Some(http::StatusCode::NOT_FOUND));
    }
}

#[tokio::test]
async fn unversioned_tagging_version_is_invalid_argument() {
    let fixture = fixture().await;

    for error in [
        get_object_tagging(&fixture.state, get_version_request(KEY, Some("null")))
            .await
            .expect_err("unversioned GetObjectTagging version must fail"),
        put_object_tagging_at(
            &fixture.state,
            put_version_request(&[("unexpected", "mutation")], Some("null")),
            time(2),
        )
        .await
        .expect_err("unversioned PutObjectTagging version must fail"),
        delete_object_tagging_at(
            &fixture.state,
            delete_version_request(KEY, Some("null")),
            time(2),
        )
        .await
        .expect_err("unversioned DeleteObjectTagging version must fail"),
    ] {
        assert_eq!(error.code().as_str(), "InvalidArgument");
        assert_eq!(error.status_code(), Some(http::StatusCode::BAD_REQUEST));
    }
}
