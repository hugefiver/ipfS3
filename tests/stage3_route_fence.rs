//! A stale accepted capture cannot publish against a different gateway's DB route.
use std::{collections::HashMap, sync::Arc};

use chrono::Utc;
use ipfs_s3_gateway::{
    config::{Config, OptionalPinControlMode},
    crypto::key::MasterKey,
    import::{
        ImportConfig, ImportSource,
        downloader::SourceDownloader,
        pipeline::{ImportCoordinator, JobCancellation, execute_job},
    },
    kubo::KuboClient,
    pinning::{
        config::{ProviderMode, ValidatedPinningConfig},
        coordinator::PinningCoordinator,
        decision::{DecisionOrigin, ExtensionDecision},
        policy::{PublicationContext, PublicationPolicy},
        tags::ObjectTag,
    },
    s3::ops::multipart::{complete_multipart_upload, create_multipart_upload},
    state::AppState,
    store::{
        self, Store,
        entities::{
            import_destination, object, object_version, pin_extension_decision, pin_job, pin_lease,
            pin_lease_target, pin_provider_usage, remote_pin, remote_pin_ledger,
            standard_mutation_lease,
        },
        import::{jobs as import_jobs, ownership as import_ownership},
        pinning::{
            ledger,
            publication::{
                self, DecidedPublish, PinTargetSpec, PublicationObject, PublicationRequest,
            },
        },
    },
};
use s3s::S3Request;
use s3s::dto::{
    CompleteMultipartUploadInput, CompletedMultipartUpload, CompletedPart,
    CreateMultipartUploadInput, ETag,
};
use sea_orm::{Database, EntityTrait, PaginatorTrait};
use tokio_util::sync::CancellationToken;
use wiremock::MockServer;

pub(crate) const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

pub(crate) fn runtime() -> std::sync::Arc<PinningCoordinator> {
    let config: Config = toml::from_str(
        r#"
        [pinning_identity]
        primary_storage_domain = 'local'
        [[pinning_identity.providers]]
        config_name = 'remote'
        provider_id = 'stable-route'
        display_name = 'Remote'
        backend = 'filebase'
        scope = 'shared-account'
        storage_domain = 'remote'
        credential_revision = 1
        endpoint_revision = 1
        secret_ref = 'env:ROUTE_TOKEN'
        api_profile = 'filebase-psa'
        strategy = 'cid'
        cleanup = 'retain'
        [[pinning.providers]]
        name = 'remote'
        kind = 'filebase'
        token_env = 'ROUTE_TOKEN'
        priority = 1
        max_bytes = 1000
        max_pins = 100
        [[pinning.policies]]
        bucket = 'route-fence'
        trigger = 'always'
        provider_mode = 'one'
        providers = ['remote']
        default_duration = '1h'
        max_duration = '24h'
    "#,
    )
    .unwrap();
    PinningCoordinator::build_with_kubo_and_mode(
        ValidatedPinningConfig::from_config(&config, |_| Some("test-token".into())).unwrap(),
        None,
        OptionalPinControlMode::Strict,
    )
    .unwrap()
}

pub(crate) fn captured(
    runtime: &PinningCoordinator,
    id: &str,
    cid: &str,
) -> (PublicationRequest, ExtensionDecision) {
    let (policy, decision) = runtime
        .policy()
        .evaluate_publication_decision(
            PublicationContext {
                bucket: "route-fence",
                key: id,
                tags: &[],
                is_decompress_zip: false,
            },
            DecisionOrigin::new("principal", id),
        )
        .unwrap();
    let PublicationPolicy { tags, .. } = &policy;
    (
        PublicationRequest {
            object: PublicationObject::from_put(
                id.into(),
                "route-fence",
                id,
                cid.into(),
                1,
                None,
                None,
                false,
                None,
                None,
                Utc::now(),
            ),
            tags: tags.clone(),
            object_target: PinTargetSpec {
                cid: cid.into(),
                logical_size: 1,
            },
            policy,
        },
        decision,
    )
}

pub(crate) async fn counts(db: &sea_orm::DatabaseConnection) -> [u64; 8] {
    [
        object::Entity::find().count(db).await.unwrap(),
        object_version::Entity::find().count(db).await.unwrap(),
        pin_extension_decision::Entity::find()
            .count(db)
            .await
            .unwrap(),
        pin_lease::Entity::find().count(db).await.unwrap(),
        pin_job::Entity::find().count(db).await.unwrap(),
        remote_pin::Entity::find().count(db).await.unwrap(),
        pin_lease_target::Entity::find().count(db).await.unwrap(),
        remote_pin_ledger::Entity::find().count(db).await.unwrap(),
    ]
}

#[tokio::test]
async fn missing_registered_remote_route_fails_preflight_and_publication() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "route-fence", None)
        .await
        .unwrap();
    let runtime = runtime();
    let (request, decision) = captured(&runtime, "missing", CID);
    let args = || DecidedPublish {
        decision: &decision,
        config: runtime.effective_config(),
        mode: runtime.control_mode(),
        limits: runtime.provider_limits(),
    };
    assert!(
        publication::preflight_decided_routes(&db, args())
            .await
            .is_err()
    );
    assert!(
        publication::publish_decided_object(&db, request, args())
            .await
            .is_err()
    );
    assert_eq!(counts(&db).await, [0; 8]);
}

#[tokio::test]
async fn one_and_all_validate_every_executable_candidate_before_any_allocation() {
    for mode in [ProviderMode::One, ProviderMode::All] {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        store::run_migrations(&db).await.unwrap();
        store::bucket::create(&db, "route-fence", None)
            .await
            .unwrap();
        let mut config = runtime().effective_config().clone();
        let mut second = config.providers[0].clone();
        second.identity.provider_id = "second-stable-route".into();
        second.identity.scope = "other-account".into();
        second.name = second.identity.allocation_key();
        config.policies[0].provider_mode = mode;
        config.policies[0].providers.push(second.name.clone());
        config.providers.push(second);
        let runtime = PinningCoordinator::build(config).unwrap();
        for provider in &runtime.effective_config().providers {
            ledger::register_route(&db, &provider.name, &provider.identity)
                .await
                .unwrap();
        }
        let second = runtime
            .effective_config()
            .providers
            .iter()
            .find(|provider| provider.identity.scope == "other-account")
            .unwrap();
        let mut r2 = second.identity.clone();
        r2.endpoint_revision += 1;
        ledger::register_route(&db, &second.name, &r2)
            .await
            .unwrap();
        let (request, decision) = captured(&runtime, "two-providers", CID);
        assert!(
            publication::preflight_decided_routes(
                &db,
                DecidedPublish {
                    decision: &decision,
                    config: runtime.effective_config(),
                    mode: runtime.control_mode(),
                    limits: runtime.provider_limits(),
                }
            )
            .await
            .is_err(),
            "mode={mode:?}"
        );
        assert!(
            publication::publish_decided_object(
                &db,
                request,
                DecidedPublish {
                    decision: &decision,
                    config: runtime.effective_config(),
                    mode: runtime.control_mode(),
                    limits: runtime.provider_limits(),
                }
            )
            .await
            .is_err(),
            "mode={mode:?}"
        );
        assert_eq!(counts(&db).await, [0; 8], "mode={mode:?}");
    }
}

#[tokio::test]
async fn db_route_override_rolls_back_new_and_shared_cid_decided_publications() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "route-fence", None)
        .await
        .unwrap();
    let runtime = runtime();
    let provider = &runtime.effective_config().providers[0];
    let key = provider.identity.allocation_key();
    ledger::register_route(&db, &key, &provider.identity)
        .await
        .unwrap();
    let (seed, seed_decision) = captured(&runtime, "seed", CID);
    publication::publish_decided_object(
        &db,
        seed,
        DecidedPublish {
            decision: &seed_decision,
            config: runtime.effective_config(),
            mode: runtime.control_mode(),
            limits: runtime.provider_limits(),
        },
    )
    .await
    .unwrap();

    let mut r2 = provider.identity.clone();
    r2.credential_revision += 1;
    ledger::register_route(&db, &key, &r2).await.unwrap();
    let before = counts(&db).await;
    let usage_before = pin_provider_usage::Entity::find_by_id(&key)
        .one(&db)
        .await
        .unwrap();
    for (id, cid) in [("new", "bafy-new-route-fence"), ("shared", CID)] {
        let (request, decision) = captured(&runtime, id, cid);
        decision
            .verify_revision(runtime.effective_config(), runtime.control_mode())
            .unwrap();
        let result = publication::publish_decided_object(
            &db,
            request,
            DecidedPublish {
                decision: &decision,
                config: runtime.effective_config(),
                mode: runtime.control_mode(),
                limits: runtime.provider_limits(),
            },
        )
        .await;
        assert!(
            result.is_err(),
            "R1 capture published {id} under registered R2"
        );
        assert_eq!(counts(&db).await, before, "partial publication for {id}");
        assert_eq!(
            pin_provider_usage::Entity::find_by_id(&key)
                .one(&db)
                .await
                .unwrap(),
            usage_before
        );
    }
}

#[tokio::test]
async fn enabled_legacy_noop_capture_rejects_same_name_real_route_before_mpu_io_and_in_transaction()
{
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "route-fence", None)
        .await
        .unwrap();
    let config: Config = toml::from_str(
        r#"
        [[pinning.providers]]
        name = 'backup'
        kind = 'noop'
        priority = 1
        max_bytes = 1000
        max_pins = 100
        [[pinning.policies]]
        bucket = 'route-fence'
        trigger = 'request'
        provider_mode = 'one'
        providers = ['backup']
        default_duration = '1h'
        max_duration = '24h'
    "#,
    )
    .unwrap();
    let runtime =
        PinningCoordinator::build(ValidatedPinningConfig::from_config(&config, |_| None).unwrap())
            .unwrap();
    let store = Store::new(db);
    runtime.register_identities(&store).await.unwrap();
    let kubo = MockServer::start().await;
    let state = Arc::new(AppState {
        kubo: KuboClient::new(kubo.uri()),
        cold_kubo: None,
        store: store.clone(),
        credentials: HashMap::new(),
        master_key: MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: runtime.clone(),
    });
    let tagging = "ipfs-s3%3Apin=true";
    let create = S3Request {
        input: CreateMultipartUploadInput {
            bucket: "route-fence".into(),
            key: "mpu".into(),
            ..Default::default()
        },
        method: http::Method::POST,
        uri: "/route-fence/mpu?uploads".parse().unwrap(),
        headers: http::HeaderMap::from_iter([(
            "x-amz-tagging".parse().unwrap(),
            tagging.parse().unwrap(),
        )]),
        extensions: http::Extensions::new(),
        credentials: Some(s3s::auth::Credentials {
            access_key: "test".into(),
            secret_key: s3s::auth::SecretKey::from("test"),
        }),
        region: None,
        service: None,
        trailing_headers: None,
    };
    let upload_id = create_multipart_upload(&state, create)
        .await
        .unwrap()
        .output
        .upload_id
        .unwrap();
    let upload = store::multipart::get_upload(store.db(), &upload_id)
        .await
        .unwrap();
    let decision = store::multipart::decision_from_upload(&upload)
        .unwrap()
        .unwrap();
    assert_eq!(
        decision.effect,
        ipfs_s3_gateway::pinning::decision::DecisionEffect::Accepted
    );
    store::multipart::upsert_part(store.db(), &upload_id, 1, CID, 1, CID)
        .await
        .unwrap();

    // Another gateway has an executable network provider with the same legacy
    // allocation key. A's in-memory revision remains unchanged.
    let mut b_config = config;
    b_config.pinning.providers[0].kind = "pinata".into();
    b_config.pinning.providers[0].token_env = Some("ROUTE_TOKEN".into());
    let b = ValidatedPinningConfig::from_config(&b_config, |_| Some("test-token".into())).unwrap();
    ledger::register_route(store.db(), "backup", &b.providers[0].identity)
        .await
        .unwrap();
    decision
        .verify_revision(runtime.effective_config(), runtime.control_mode())
        .unwrap();
    let args = || DecidedPublish {
        decision: &decision,
        config: runtime.effective_config(),
        mode: runtime.control_mode(),
        limits: runtime.provider_limits(),
    };
    let before = counts(store.db()).await;
    let usage_before = pin_provider_usage::Entity::find_by_id("backup")
        .one(store.db())
        .await
        .unwrap();
    assert!(
        publication::preflight_decided_routes(store.db(), args())
            .await
            .is_err()
    );
    let complete = S3Request {
        input: CompleteMultipartUploadInput {
            bucket: "route-fence".into(),
            key: "mpu".into(),
            upload_id: upload_id.clone(),
            multipart_upload: Some(CompletedMultipartUpload {
                parts: Some(vec![CompletedPart {
                    part_number: Some(1),
                    e_tag: Some(ETag::Strong(CID.into())),
                    ..Default::default()
                }]),
            }),
            ..Default::default()
        },
        method: http::Method::POST,
        uri: format!("/route-fence/mpu?uploadId={upload_id}")
            .parse()
            .unwrap(),
        headers: http::HeaderMap::new(),
        extensions: http::Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    };
    assert!(complete_multipart_upload(&state, complete).await.is_err());
    assert!(kubo.received_requests().await.unwrap().is_empty());
    assert_eq!(
        standard_mutation_lease::Entity::find()
            .count(store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        import_destination::Entity::find()
            .count(store.db())
            .await
            .unwrap(),
        0
    );
    assert!(
        store::multipart::get_part(store.db(), &upload_id, 1)
            .await
            .is_ok()
    );

    let policy = decision
        .replay_policy(vec![ipfs_s3_gateway::pinning::tags::ObjectTag::new(
            "ipfs-s3:pin",
            "true",
        )])
        .unwrap();
    let request = PublicationRequest {
        object: PublicationObject::from_put(
            "noop-final".into(),
            "route-fence",
            "new",
            CID.into(),
            1,
            None,
            None,
            false,
            None,
            None,
            Utc::now(),
        ),
        tags: policy.tags.clone(),
        object_target: PinTargetSpec {
            cid: CID.into(),
            logical_size: 1,
        },
        policy,
    };
    assert!(
        publication::publish_decided_object(store.db(), request, args())
            .await
            .is_err()
    );
    assert_eq!(counts(store.db()).await, before);
    assert_eq!(
        pin_provider_usage::Entity::find_by_id("backup")
            .one(store.db())
            .await
            .unwrap(),
        usage_before
    );

    // A separate accepted import on the same gateway must also stop before its
    // CID source contacts Kubo (including the ZIP path, which starts with cat).
    let tags = vec![ObjectTag::new("ipfs-s3:pin", "true")];
    let (_, mut import_decision) = runtime
        .policy()
        .evaluate_publication_decision(
            PublicationContext {
                bucket: "route-fence",
                key: "import",
                tags: &tags,
                is_decompress_zip: false,
            },
            DecisionOrigin::new("principal", "noop-import"),
        )
        .unwrap();
    import_decision
        .capture_durable_revision(runtime.effective_config(), runtime.control_mode())
        .unwrap();
    let now = Utc::now();
    import_ownership::submit_decided(
        store.db(),
        import_jobs::NewImportJob {
            id: "noop-import".into(),
            bucket: "route-fence".into(),
            key: "import".into(),
            source: ImportSource::Cid(CID.into()),
            request_fingerprint: "noop-fingerprint".into(),
            client_token: None,
            object_content_type: None,
            metadata: HashMap::new(),
            tags,
            decompress_prefix: None,
        },
        import_decision,
        now,
    )
    .await
    .unwrap();
    let claimed = import_jobs::claim_due(
        store.db(),
        "worker",
        now,
        now + chrono::TimeDelta::seconds(60),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    let destination_before =
        import_destination::Entity::find_by_id(("route-fence".into(), "import".into()))
            .one(store.db())
            .await
            .unwrap();
    let import_config = ImportConfig::default().validate().unwrap();
    let coordinator = ImportCoordinator::new(
        import_config.clone(),
        SourceDownloader::production(Arc::new(import_config)),
    );
    let cancellation = JobCancellation {
        shutdown: CancellationToken::new(),
        ownership_lost: CancellationToken::new(),
    };
    assert!(
        execute_job(
            coordinator,
            state.clone(),
            claimed.job,
            claimed.claim,
            cancellation
        )
        .await
        .is_err()
    );
    assert_eq!(
        import_destination::Entity::find_by_id(("route-fence".into(), "import".into()))
            .one(store.db())
            .await
            .unwrap(),
        destination_before
    );
    assert!(kubo.received_requests().await.unwrap().is_empty());
    assert_eq!(counts(store.db()).await, before);

    // Normal Noop is still permitted when its own registered route is current.
    runtime.register_identities(&store).await.unwrap();
    let policy = decision
        .replay_policy(vec![ipfs_s3_gateway::pinning::tags::ObjectTag::new(
            "ipfs-s3:pin",
            "true",
        )])
        .unwrap();
    publication::publish_decided_object(
        store.db(),
        PublicationRequest {
            object: PublicationObject::from_put(
                "noop-good".into(),
                "route-fence",
                "new",
                CID.into(),
                1,
                None,
                None,
                false,
                None,
                None,
                Utc::now(),
            ),
            tags: policy.tags.clone(),
            object_target: PinTargetSpec {
                cid: CID.into(),
                logical_size: 1,
            },
            policy,
        },
        args(),
    )
    .await
    .unwrap();
    assert_eq!(counts(store.db()).await[0], before[0] + 1);
}
