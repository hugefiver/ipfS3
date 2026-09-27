use super::*;
use crate::config::{OptionalPinControlMode, PinningConfig, PolicyConfig, ProviderConfig};
use crate::pinning::config::ValidatedPinningConfig;
use crate::s3::sigv4;
use crate::store::entities::{import_destination, import_job};
use crate::store::entities::{pin_job, pin_lease, pin_provider_usage, remote_pin};
use sea_orm::{Database, EntityTrait, PaginatorTrait};

async fn queued_import(state: &Arc<AppState>, id: &str, key: &str) {
    let result = crate::store::import::ownership::submit(
        state.store.db(),
        crate::store::import::jobs::NewImportJob {
            id: id.into(),
            bucket: "bucket".into(),
            key: key.into(),
            source: crate::import::ImportSource::Cid(CID.into()),
            request_fingerprint: format!("sha256:{id}"),
            client_token: None,
            object_content_type: None,
            metadata: HashMap::new(),
            tags: Vec::new(),
            decompress_prefix: None,
        },
        chrono::Utc::now(),
    )
    .await
    .unwrap();
    assert!(matches!(
        result,
        crate::store::import::jobs::SubmitImportOutcome::Created(_)
    ));
}

async fn assert_import_still_owns(state: &Arc<AppState>, id: &str, key: &str) {
    let db = state.store.db();
    assert_eq!(
        import_job::Entity::find_by_id(id)
            .one(db)
            .await
            .unwrap()
            .unwrap()
            .state,
        "queued"
    );
    let destination = import_destination::Entity::find_by_id(("bucket".to_owned(), key.to_owned()))
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(destination.owner_job_id.as_deref(), Some(id));
    assert!(destination.mutation_id.is_none());
}

#[tokio::test]
async fn invalid_put_and_copy_leave_queued_imports_and_existing_writer_untouched() {
    let kubo = kubo_server(CID).await;
    let state = pinning_state(kubo.uri(), "request", "one", "").await;
    seed_copy_source(&state, "source", CID, &[]).await;
    queued_import(&state, "put-import", "put-target").await;
    queued_import(&state, "copy-import", "copy-target").await;
    let writer = crate::store::import::ownership::admit_content_mutation(
        state.store.db(),
        "bucket",
        "writer-target",
        None,
        crate::import::SupersedeReason::PutObject,
        chrono::Utc::now(),
    )
    .await
    .unwrap();

    for (key, control) in [
        ("put-target", "ipfs-s3%3Apin=TRUE"),
        ("writer-target", "ipfs-s3%3Apin=true&ipfs-s3%3Aduration=25h"),
    ] {
        let error = put_object(&state, put_request(key, Some(control)))
            .await
            .unwrap_err();
        assert_eq!(error.code().as_str(), "InvalidArgument");
    }
    let error = copy_object(
        &state,
        copy_request(
            "source",
            "copy-target",
            Some("REPLACE"),
            Some("ipfs-s3%3Apin=true&ipfs-s3%3Aduration=25h"),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code().as_str(), "InvalidArgument");
    assert_import_still_owns(&state, "put-import", "put-target").await;
    assert_import_still_owns(&state, "copy-import", "copy-target").await;
    let current =
        import_destination::Entity::find_by_id(("bucket".to_owned(), "writer-target".to_owned()))
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(
        current.mutation_id.as_deref(),
        Some(writer.mutation_id.as_str())
    );
    assert_eq!(current.generation, writer.expected_generation);
    assert!(kubo.received_requests().await.unwrap().is_empty());
    crate::store::import::ownership::release_standard_mutation(state.store.db(), &writer)
        .await
        .unwrap();
}

#[tokio::test]
async fn missing_bucket_and_copy_source_precede_pinning_preflight() {
    let kubo = kubo_server(CID).await;
    let state = pinning_state(kubo.uri(), "request", "one", "").await;
    let mut put = put_request("key", Some("ipfs-s3%3Apin=TRUE"));
    put.input.bucket = "missing".into();
    assert_eq!(
        put_object(&state, put).await.unwrap_err().code().as_str(),
        "NoSuchBucket"
    );

    let mut copy = copy_request(
        "absent",
        "dest",
        Some("REPLACE"),
        Some("ipfs-s3%3Apin=TRUE"),
    );
    copy.input.bucket = "missing".into();
    assert_eq!(
        copy_object(&state, copy).await.unwrap_err().code().as_str(),
        "NoSuchKey"
    );
    assert!(kubo.received_requests().await.unwrap().is_empty());
}

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

async fn warn_state(uri: String) -> Arc<AppState> {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    crate::store::bucket::create(&db, "bucket", None)
        .await
        .unwrap();
    let config = ValidatedPinningConfig::from_raw(&PinningConfig::default(), |_| None).unwrap();
    Arc::new(AppState {
        kubo: crate::kubo::KuboClient::new(uri),
        cold_kubo: None,
        store: crate::store::Store::new(db),
        credentials: HashMap::from([("test".into(), s3s::auth::SecretKey::from("test"))]),
        master_key: crate::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: crate::pinning::coordinator::PinningCoordinator::build_with_kubo_and_mode(
            config,
            None,
            OptionalPinControlMode::Warn,
        )
        .unwrap(),
    })
}

#[tokio::test]
async fn real_sigv4_put_copy_and_tagging_return_standard_responses_with_safe_warnings() {
    use axum::error_handling::HandleError;
    use s3s::service::S3ServiceBuilder;

    let kubo = kubo_server(CID).await;
    let state = warn_state(kubo.uri()).await;
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
    headers.insert(
        "x-amz-tagging",
        "ipfs-s3%3Apin=true&private=do-not-leak".parse().unwrap(),
    );
    let put = sigv4::send_sigv4(
        reqwest::Method::PUT,
        &endpoint,
        "bucket",
        "signed-source",
        &[],
        b"body".to_vec(),
        headers,
        "test",
    )
    .await;
    assert_eq!(put.status(), reqwest::StatusCode::OK);
    assert_eq!(
        put.headers()["x-ipfs3-pin-warning"],
        "pin-policy-unavailable"
    );
    assert!(put.headers()["etag"].to_str().unwrap().contains(CID));
    assert!(!put.text().await.unwrap().contains("do-not-leak"));
    let mut headers = http::HeaderMap::new();
    headers.insert(
        "x-amz-copy-source",
        "/bucket/signed-source".parse().unwrap(),
    );
    let copy = sigv4::send_sigv4(
        reqwest::Method::PUT,
        &endpoint,
        "bucket",
        "signed-copy",
        &[],
        Vec::new(),
        headers,
        "test",
    )
    .await;
    assert_eq!(copy.status(), reqwest::StatusCode::OK);
    assert_eq!(
        copy.headers()["x-ipfs3-pin-warning"],
        "pin-policy-unavailable"
    );
    let xml = copy.text().await.unwrap();
    assert!(xml.contains(CID));

    let mut replace_headers = http::HeaderMap::new();
    replace_headers.insert(
        "x-amz-copy-source",
        "/bucket/signed-source".parse().unwrap(),
    );
    replace_headers.insert("x-amz-tagging-directive", "REPLACE".parse().unwrap());
    replace_headers.insert("x-amz-tagging", "ipfs-s3%3Apin=true".parse().unwrap());
    let replaced = sigv4::send_sigv4(
        reqwest::Method::PUT,
        &endpoint,
        "bucket",
        "signed-replaced",
        &[],
        Vec::new(),
        replace_headers,
        "test",
    )
    .await;
    assert_eq!(replaced.status(), reqwest::StatusCode::OK);
    assert_eq!(
        replaced.headers()["x-ipfs3-pin-warning"],
        "pin-policy-unavailable"
    );
    assert!(replaced.text().await.unwrap().contains(CID));

    let tagging_xml = "<Tagging><TagSet><Tag><Key>ipfs-s3:pin</Key><Value>true</Value></Tag><Tag><Key>private</Key><Value>do-not-leak</Value></Tag></TagSet></Tagging>";
    let tagging = sigv4::send_sigv4(
        reqwest::Method::PUT,
        &endpoint,
        "bucket",
        "signed-copy",
        &[("tagging", "")],
        tagging_xml.as_bytes().to_vec(),
        http::HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(tagging.status(), reqwest::StatusCode::OK);
    assert_eq!(
        tagging.headers()["x-ipfs3-pin-warning"],
        "pin-policy-unavailable"
    );
    assert!(tagging.text().await.unwrap().is_empty());
    let versions = object_version::Entity::find()
        .all(state.store.db())
        .await
        .unwrap();
    assert_eq!(versions.len(), 3);
    for version in &versions {
        let decision =
            crate::store::pinning::decision::read_for_version(state.store.db(), &version.id)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            decision.effect,
            crate::pinning::decision::DecisionEffect::Skipped
        );
        assert_eq!(decision.origin.principal_id, "test");
        uuid::Uuid::parse_str(&decision.origin.request_id).unwrap();
    }
    assert_no_remote_work(&state).await;
    server.abort();
}

async fn assert_no_remote_work(state: &Arc<AppState>) {
    let db = state.store.db();
    assert_eq!(pin_lease::Entity::find().count(db).await.unwrap(), 0);
    assert_eq!(pin_job::Entity::find().count(db).await.unwrap(), 0);
    assert_eq!(remote_pin::Entity::find().count(db).await.unwrap(), 0);
    assert_eq!(
        pin_provider_usage::Entity::find().count(db).await.unwrap(),
        0
    );
}

fn available_policy() -> Arc<crate::pinning::coordinator::PinningCoordinator> {
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
    crate::pinning::coordinator::PinningCoordinator::build(
        ValidatedPinningConfig::from_raw(&raw, |_| None).unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn warn_disabled_provider_only_skips_manual_without_remote_work() {
    let kubo = kubo_server(CID).await;
    let base = warn_state(kubo.uri()).await;
    let mut raw = PinningConfig::default();
    raw.providers.push(ProviderConfig {
        name: "disabled".into(),
        kind: "noop".into(),
        token_env: None,
        endpoint: None,
        api: None,
        strategy: None,
        upload_endpoint: None,
        enabled: false,
        priority: 1,
        max_bytes: 1000,
        max_pins: 100,
        requests_per_second: None,
    });
    raw.policies.push(PolicyConfig {
        bucket: "bucket".into(),
        prefix: String::new(),
        trigger: "request".into(),
        provider_mode: "all".into(),
        providers: vec!["disabled".into()],
        default_duration: "1h".into(),
        max_duration: "24h".into(),
        allow_decompressed: false,
    });
    let mut cfg = crate::config::Config::default_for_test();
    cfg.pinning = raw;
    cfg.pinning_control.unavailable = OptionalPinControlMode::Warn;
    let validated = ValidatedPinningConfig::from_config(&cfg, |_| None).unwrap();
    let state = Arc::new(AppState {
        kubo: crate::kubo::KuboClient::new(kubo.uri()),
        cold_kubo: None,
        store: crate::store::Store::new(base.store.db().clone()),
        credentials: HashMap::new(),
        master_key: crate::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: crate::pinning::coordinator::PinningCoordinator::build_with_kubo_and_mode(
            validated,
            None,
            OptionalPinControlMode::Warn,
        )
        .unwrap(),
    });
    let response = put_object(&state, put_request("disabled", Some("ipfs-s3%3Apin=true")))
        .await
        .unwrap();
    assert_eq!(
        response.headers["x-ipfs3-pin-warning"],
        "pin-provider-unavailable"
    );
    assert_no_remote_work(&state).await;
}

#[tokio::test]
async fn warn_manual_skip_never_swallows_automatic_policy_failure() {
    let kubo = kubo_server(CID).await;
    let base = warn_state(kubo.uri()).await;
    let mut config = available_policy().effective_config().clone();
    config.policies[0].trigger = crate::pinning::config::PolicyTrigger::Always;
    let provider = config.policies[0].providers[0].clone();
    config.provider_limits.get_mut(&provider).unwrap().enabled = false;
    for route in &mut config.providers {
        route.limits.enabled = false;
    }
    let state = Arc::new(AppState {
        kubo: crate::kubo::KuboClient::new(kubo.uri()),
        cold_kubo: None,
        store: crate::store::Store::new(base.store.db().clone()),
        credentials: HashMap::new(),
        master_key: crate::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: crate::pinning::coordinator::PinningCoordinator::build_with_kubo_and_mode(
            config,
            None,
            OptionalPinControlMode::Warn,
        )
        .unwrap(),
    });
    let error = put_object(
        &state,
        put_request("auto-fails", Some("ipfs-s3%3Apin=true")),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code().as_str(), "InvalidArgument");
    assert!(
        crate::store::object::get_latest(state.store.db(), "bucket", "auto-fails")
            .await
            .is_err()
    );
    assert_no_remote_work(&state).await;
}

#[tokio::test]
async fn malformed_manual_control_never_starts_kubo_upload_in_warn_mode() {
    let kubo = kubo_server(CID).await;
    let state = warn_state(kubo.uri()).await;
    let error = put_object(&state, put_request("invalid", Some("ipfs-s3%3Apin=TRUE")))
        .await
        .unwrap_err();
    assert_eq!(error.code().as_str(), "InvalidArgument");
    assert!(kubo.received_requests().await.unwrap().is_empty());
    assert_no_remote_work(&state).await;
}

#[tokio::test]
async fn warn_put_persists_skipped_decision_without_remote_work() {
    let kubo = kubo_server(CID).await;
    let state = warn_state(kubo.uri()).await;
    let mut req = put_request("skipped", Some("ipfs-s3%3Apin=true&private=secret"));
    req.credentials = Some(s3s::auth::Credentials {
        access_key: "owner-key".into(),
        secret_key: s3s::auth::SecretKey::from("test"),
    });

    let response = put_object(&state, req).await.unwrap();
    assert_eq!(response.output.e_tag.as_ref().map(ETag::value), Some(CID));
    assert_eq!(
        response.headers["x-ipfs3-pin-warning"],
        "pin-policy-unavailable"
    );
    let object = crate::store::object::get_latest(state.store.db(), "bucket", "skipped")
        .await
        .unwrap();
    let version = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(&object.id))
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    let decision = crate::store::pinning::decision::read_for_version(state.store.db(), &version.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        decision.effect,
        crate::pinning::decision::DecisionEffect::Skipped
    );
    assert_eq!(decision.origin.principal_id, "owner-key");
    assert!(!serde_json::to_string(&decision).unwrap().contains("secret"));
    assert_no_remote_work(&state).await;
}

#[tokio::test]
async fn copied_skipped_tags_remain_skipped_after_policy_becomes_available() {
    let kubo = kubo_server(CID).await;
    let state = warn_state(kubo.uri()).await;
    let mut put = put_request("source", Some("ipfs-s3%3Apin=true&team=infra"));
    put.credentials = Some(s3s::auth::Credentials {
        access_key: "owner-key".into(),
        secret_key: s3s::auth::SecretKey::from("test"),
    });
    put_object(&state, put).await.unwrap();

    let available = Arc::new(AppState {
        kubo: crate::kubo::KuboClient::new(kubo.uri()),
        cold_kubo: None,
        store: crate::store::Store::new(state.store.db().clone()),
        credentials: HashMap::new(),
        master_key: crate::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: available_policy(),
    });
    let mut copy = copy_request("source", "destination", None, None);
    copy.credentials = Some(s3s::auth::Credentials {
        access_key: "copy-owner".into(),
        secret_key: s3s::auth::SecretKey::from("test"),
    });
    let response = copy_object(&available, copy).await.unwrap();
    assert_eq!(
        response.headers["x-ipfs3-pin-warning"],
        "pin-policy-unavailable"
    );
    assert_eq!(
        response
            .output
            .copy_object_result
            .unwrap()
            .e_tag
            .unwrap()
            .value(),
        CID
    );
    let source = crate::store::object::get_latest(available.store.db(), "bucket", "source")
        .await
        .unwrap();
    let dest = crate::store::object::get_latest(available.store.db(), "bucket", "destination")
        .await
        .unwrap();
    let versions = object_version::Entity::find()
        .all(available.store.db())
        .await
        .unwrap();
    let source_decision = crate::store::pinning::decision::read_for_version(
        available.store.db(),
        &versions
            .iter()
            .find(|v| v.object_id.as_deref() == Some(&source.id))
            .unwrap()
            .id,
    )
    .await
    .unwrap()
    .unwrap();
    let destination_decision = crate::store::pinning::decision::read_for_version(
        available.store.db(),
        &versions
            .iter()
            .find(|v| v.object_id.as_deref() == Some(&dest.id))
            .unwrap()
            .id,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        destination_decision.effect,
        crate::pinning::decision::DecisionEffect::Skipped
    );
    assert_eq!(destination_decision.origin.principal_id, "copy-owner");
    assert_ne!(
        source_decision.control_revision,
        destination_decision.control_revision
    );
    assert_eq!(
        crate::store::pinning::tags::list_object_tags(available.store.db(), &dest.id)
            .await
            .unwrap(),
        vec![
            ObjectTag::new("ipfs-s3:pin", "true"),
            ObjectTag::new("team", "infra")
        ]
    );
    assert_no_remote_work(&available).await;
}

#[tokio::test]
async fn signed_copy_of_legacy_raw_control_preserves_tags_without_manual_work() {
    use axum::error_handling::HandleError;
    use s3s::service::S3ServiceBuilder;

    let kubo = kubo_server(CID).await;
    let mut state = pinning_state(kubo.uri(), "request", "one", "dest/").await;
    Arc::get_mut(&mut state)
        .unwrap()
        .credentials
        .insert("test".into(), s3s::auth::SecretKey::from("test"));
    seed_copy_source(
        &state,
        "source",
        CID,
        &[
            ObjectTag::new("ipfs-s3:pin", "true"),
            ObjectTag::new("team", "legacy"),
        ],
    )
    .await;
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

    let mut prior_revision = None;
    for (source, dest, directive) in [
        ("source", "dest/default", None),
        ("dest/default", "dest/chained", Some("COPY")),
    ] {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "x-amz-copy-source",
            format!("/bucket/{source}").parse().unwrap(),
        );
        if let Some(directive) = directive {
            headers.insert("x-amz-tagging-directive", directive.parse().unwrap());
        }
        let response = sigv4::send_sigv4(
            reqwest::Method::PUT,
            &endpoint,
            "bucket",
            dest,
            &[],
            Vec::new(),
            headers,
            "test",
        )
        .await;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert!(!response.headers().contains_key("x-ipfs3-pin-warning"));
        assert!(response.text().await.unwrap().contains(CID));
        let object = crate::store::object::get_latest(state.store.db(), "bucket", dest)
            .await
            .unwrap();
        assert_eq!(object.cid, CID);
        assert_eq!(
            crate::store::pinning::tags::list_object_tags(state.store.db(), &object.id)
                .await
                .unwrap(),
            vec![
                ObjectTag::new("ipfs-s3:pin", "true"),
                ObjectTag::new("team", "legacy")
            ],
        );
        let version = object_version::Entity::find()
            .filter(object_version::Column::ObjectId.eq(&object.id))
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        let decision =
            crate::store::pinning::decision::read_for_version(state.store.db(), &version.id)
                .await
                .unwrap()
                .unwrap();
        assert!(decision.legacy_unknown);
        assert_eq!(decision.effect, DecisionEffect::NoIntent);
        assert_eq!(decision.warning, None);
        assert_eq!(decision.origin.principal_id, "test");
        uuid::Uuid::parse_str(&decision.origin.request_id).unwrap();
        assert_ne!(prior_revision.as_ref(), Some(&decision.control_revision));
        prior_revision = Some(decision.control_revision.clone());
        assert!(decision.effective_intents.is_empty());
        decision
            .replay_policy(vec![
                ObjectTag::new("ipfs-s3:pin", "true"),
                ObjectTag::new("team", "legacy"),
            ])
            .unwrap();
        assert_no_remote_work(&state).await;
    }

    let requests = kubo.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| request.url.path() == "/api/v0/pin/add")
    );
    server.abort();
}

#[tokio::test]
async fn strict_copy_of_legacy_control_only_runs_destination_automatic_policy() {
    let kubo = kubo_server(CID).await;
    let state = pinning_state(kubo.uri(), "always", "one", "dest/").await;
    seed_copy_source(
        &state,
        "source",
        CID,
        &[ObjectTag::new("ipfs-s3:pin", "true")],
    )
    .await;

    let response = copy_object(&state, copy_request("source", "dest/automatic", None, None))
        .await
        .unwrap();
    assert!(!response.headers.contains_key("x-ipfs3-pin-warning"));
    let object = crate::store::object::get_latest(state.store.db(), "bucket", "dest/automatic")
        .await
        .unwrap();
    let version = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(&object.id))
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    let decision = crate::store::pinning::decision::read_for_version(state.store.db(), &version.id)
        .await
        .unwrap()
        .unwrap();
    assert!(decision.legacy_unknown);
    assert!(
        decision
            .effective_intents
            .iter()
            .all(|intent| intent.source == crate::pinning::policy::LeaseSource::Automatic)
    );
    assert_eq!(
        lease_sources_for_latest(&state, "dest/automatic").await,
        vec!["automatic"]
    );
    assert_eq!(
        pin_lease::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        pin_job::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        remote_pin::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        1
    );
    let usage = pin_provider_usage::Entity::find_by_id("alpha")
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (4, 1));
}

#[tokio::test]
async fn legacy_invalid_control_still_rejected_but_replace_is_fresh() {
    let kubo = kubo_server(CID).await;
    let state = pinning_state(kubo.uri(), "request", "one", "dest/").await;
    seed_copy_source(
        &state,
        "source",
        CID,
        &[ObjectTag::new("ipfs-s3:pin", "TRUE")],
    )
    .await;
    let error = copy_object(&state, copy_request("source", "dest/invalid", None, None))
        .await
        .unwrap_err();
    assert_eq!(error.code().as_str(), "InvalidArgument");
    assert!(
        crate::store::object::get_latest(state.store.db(), "bucket", "dest/invalid")
            .await
            .is_err()
    );
    assert!(kubo.received_requests().await.unwrap().is_empty());
    copy_object(
        &state,
        copy_request(
            "source",
            "dest/replaced",
            Some("REPLACE"),
            Some("ipfs-s3%3Apin=true"),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        lease_sources_for_latest(&state, "dest/replaced").await,
        vec!["manual"]
    );
}
