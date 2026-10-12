use chrono::Utc;
use ipfs_s3_gateway::{
    config::{
        DecompressZipConfig, OptionalPinControlMode, PinningConfig, PolicyConfig, ProviderConfig,
    },
    import::SupersedeReason,
    pinning::{
        config::ValidatedPinningConfig,
        coordinator::normalize_validated_config,
        decision::DecisionOrigin,
        tags::ObjectTag,
        zip_policy::{CapturedZipIntent, ZipCaptureContext, capture_zip_intent},
    },
    store::{
        self,
        import::ownership::{StandardMutationGuard, admit_content_and_prefix_mutation},
        pinning::publication::{
            self, DecidedPublish, PinTargetSpec, PublicationObject, PublicationRequest,
            ZipBatchEntry, ZipBatchPublication, ZipPublicationRequest,
        },
        zip::{BatchAdmission, ManifestItem, RootOutcome},
    },
    zip::options::parse_authenticated_zip_controls,
};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection};

pub fn config() -> ValidatedPinningConfig {
    normalize_validated_config(
        ValidatedPinningConfig::from_raw(
            &PinningConfig {
                providers: vec![ProviderConfig {
                    name: "remote".into(),
                    kind: "noop".into(),
                    enabled: true,
                    priority: 1,
                    max_bytes: 1000,
                    max_pins: 100,
                    cluster: None,
                    token_env: None,
                    endpoint: None,
                    api: None,
                    strategy: None,
                    upload_endpoint: None,
                    requests_per_second: None,
                }],
                policies: vec![PolicyConfig {
                    bucket: "bucket".into(),
                    prefix: "out/".into(),
                    trigger: "always".into(),
                    provider_mode: "all".into(),
                    providers: vec!["remote".into()],
                    default_duration: "1h".into(),
                    max_duration: "2h".into(),
                    allow_decompressed: true,
                }],
                ..Default::default()
            },
            |_| None,
        )
        .unwrap(),
    )
    .unwrap()
}

pub fn object(id: &str, key: &str, cid: &str) -> PublicationObject {
    PublicationObject::from_put(
        id.into(),
        "bucket",
        key,
        cid.into(),
        5,
        None,
        None,
        false,
        None,
        None,
        Utc::now(),
    )
}

pub async fn initialize(db: &DatabaseConnection) {
    store::run_migrations(db).await.unwrap();
    store::bucket::create(db, "bucket", None).await.unwrap();
}

pub async fn setup() -> DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .unwrap();
    initialize(&db).await;
    db
}

pub async fn prepared(
    db: &DatabaseConnection,
    config: &ValidatedPinningConfig,
    tags: Vec<ObjectTag>,
) -> (
    ZipPublicationRequest,
    ZipBatchPublication,
    CapturedZipIntent,
    StandardMutationGuard,
) {
    prepared_with_outputs(
        db,
        config,
        tags,
        vec![
            ("a".into(), object("a", "out/a", "shared-cid")),
            ("b".into(), object("b", "out/b", "shared-cid")),
        ],
    )
    .await
}

pub async fn prepared_with_outputs(
    db: &DatabaseConnection,
    config: &ValidatedPinningConfig,
    tags: Vec<ObjectTag>,
    outputs: Vec<(String, PublicationObject)>,
) -> (
    ZipPublicationRequest,
    ZipBatchPublication,
    CapturedZipIntent,
    StandardMutationGuard,
) {
    for provider in &config.providers {
        store::pinning::ledger::register_configured_route(
            db,
            &provider.identity.allocation_key(),
            provider,
        )
        .await
        .unwrap();
    }
    let guard = admit_content_and_prefix_mutation(
        db,
        "bucket",
        "out/archive.zip",
        "out/",
        SupersedeReason::DecompressZip,
        Utc::now(),
    )
    .await
    .unwrap();
    let controls = parse_authenticated_zip_controls(
        &http::HeaderMap::new(),
        &[("decompress-zip".into(), "out/".into())],
        &tags,
        true,
        false,
    )
    .unwrap()
    .unwrap();
    let capture = capture_zip_intent(
        config,
        &DecompressZipConfig {
            auto_pin_outputs: Some(true),
            ..Default::default()
        },
        OptionalPinControlMode::Warn,
        ZipCaptureContext {
            bucket: "bucket",
            source_key: "out/archive.zip",
            target_prefix: "out/",
            tags: &tags,
        },
        &controls,
        DecisionOrigin::new("principal", &guard.mutation_id),
    )
    .unwrap();
    store::zip::admit(
        db,
        &BatchAdmission {
            id: "batch".into(),
            owner: "principal".into(),
            source: "direct".into(),
            token: capture.controls().admission_token.clone(),
            fingerprint: "request".into(),
            bucket: "bucket".into(),
            archive_key: "out/archive.zip".into(),
            input_identity: "digest".into(),
            captured_options: serde_json::json!({"zip_intent": capture}).to_string(),
        },
    )
    .await
    .unwrap();
    store::zip::prepare_manifest(
        db,
        "batch",
        &outputs
            .iter()
            .map(|(path, e)| ManifestItem::Success {
                path: path.clone(),
                object_key: e.key.clone(),
                cid: e.cid.clone(),
                size: e.logical_size,
            })
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    let source = object("archive", "out/archive.zip", "archive-cid");
    let request = ZipPublicationRequest {
        archive: PublicationRequest {
            object_target: PinTargetSpec {
                cid: source.cid.clone(),
                logical_size: source.logical_size,
            },
            object: source,
            policy: capture
                .source_decision()
                .replay_policy(tags.clone())
                .unwrap(),
            tags,
        },
        entries: outputs.iter().map(|(_, object)| object.clone()).collect(),
    };
    let batch = ZipBatchPublication {
        batch_id: "batch".into(),
        entries: outputs
            .into_iter()
            .map(|(path, object)| ZipBatchEntry {
                path,
                object_id: object.id,
            })
            .collect(),
        source_published: true,
        root_outcome: RootOutcome::Disabled,
        terminal_result: "{}".into(),
    };
    (request, batch, capture, guard)
}

pub async fn publish(
    db: &DatabaseConnection,
    config: &ValidatedPinningConfig,
    request: ZipPublicationRequest,
    batch: ZipBatchPublication,
    capture: &CapturedZipIntent,
    guard: StandardMutationGuard,
) -> ipfs_s3_gateway::error::AppResult<publication::PublicationResult> {
    publication::publish_decided_zip_batch(
        db,
        request,
        Some(guard),
        batch,
        DecidedPublish {
            decision: capture.source_decision(),
            config,
            mode: OptionalPinControlMode::Warn,
            limits: &config.provider_limits,
        },
    )
    .await
}
