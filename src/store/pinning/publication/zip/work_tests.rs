use super::*;
use crate::{
    config::{
        DecompressZipConfig, OptionalPinControlMode, PinningConfig, PolicyConfig, ProviderConfig,
    },
    pinning::{
        config::ValidatedPinningConfig,
        coordinator::normalize_validated_config,
        decision::DecisionOrigin,
        zip_policy::{ZipCaptureContext, capture_zip_intent},
    },
    store::pinning::management::{
        ManagementError,
        native::meter::{Budget, Limits},
    },
    zip::{batch::ManifestFile, options::parse_authenticated_zip_controls},
};

fn fixture(
    text_bytes: usize,
    outputs: usize,
) -> (
    CapturedZipIntent,
    ZipBoundOutput,
    Vec<ZipBoundOutput>,
    FinalZipManifest,
) {
    let mut config = ValidatedPinningConfig::from_raw(
        &PinningConfig {
            providers: (0..4)
                .map(|n| ProviderConfig {
                    name: format!("p{n}"),
                    kind: "noop".into(),
                    enabled: true,
                    priority: n,
                    max_bytes: i64::MAX as u64,
                    max_pins: 100_000,
                    cluster: None,
                    token_env: None,
                    endpoint: None,
                    api: None,
                    strategy: None,
                    upload_endpoint: None,
                    requests_per_second: None,
                })
                .collect(),
            policies: vec![PolicyConfig {
                bucket: "bucket".into(),
                prefix: "out/".into(),
                trigger: "always".into(),
                provider_mode: "one".into(),
                providers: (0..4).map(|n| format!("p{n}")).collect(),
                default_duration: "1h".into(),
                max_duration: "2h".into(),
                allow_decompressed: true,
            }],
            ..Default::default()
        },
        |_| None,
    )
    .unwrap();
    for (n, provider) in config.providers.iter_mut().enumerate() {
        provider.identity.provider_id = format!("provider-{n}");
        provider.identity.api_profile = "psa".into();
        provider.identity.scope = format!("account-{n}");
        provider.identity.secret_ref = Some("x".repeat(text_bytes));
    }
    let config = normalize_validated_config(config).unwrap();
    let controls = parse_authenticated_zip_controls(
        &http::HeaderMap::new(),
        &[("decompress-zip".into(), "out/".into())],
        &[],
        true,
        true,
    )
    .unwrap()
    .unwrap();
    let capture = capture_zip_intent(
        &config,
        &DecompressZipConfig {
            auto_pin_outputs: Some(true),
            ..Default::default()
        },
        OptionalPinControlMode::Warn,
        ZipCaptureContext {
            bucket: "bucket",
            source_key: "out/archive.zip",
            target_prefix: "out/",
            tags: &[],
        },
        &controls,
        DecisionOrigin::new("tenant", "admission"),
    )
    .unwrap();
    let source = ZipBoundOutput {
        object_id: "source".into(),
        bucket: "bucket".into(),
        key: "out/archive.zip".into(),
        version_id: "null".into(),
        cid: "source-cid".into(),
        logical_size: 5,
    };
    let outputs = (0..outputs)
        .map(|n| ZipBoundOutput {
            object_id: format!("output-{n}"),
            bucket: "bucket".into(),
            key: format!("out/{n}"),
            version_id: format!("version-{n}"),
            cid: format!("cid-{n}"),
            logical_size: 5,
        })
        .collect::<Vec<_>>();
    let manifest = FinalZipManifest {
        successful: outputs
            .iter()
            .map(|o| ManifestFile {
                relative_path: o.key[4..].into(),
                object_key: o.key.clone(),
                cid: o.cid.clone(),
                size: o.logical_size,
            })
            .collect(),
        ..Default::default()
    };
    (capture, source, outputs, manifest)
}

#[test]
fn large_legal_provider_text_is_rejected_before_selection_expands_outputs() {
    let (capture, source, outputs, mut manifest) = fixture(256 * 1024, 256);
    let encoded = serde_json::to_string(&capture).unwrap();
    assert!(encoded.len() < crate::pinning::management::wire::MAX_WIRE_BYTES);
    CapturedZipIntent::decode_json(&encoded).unwrap();
    let session = std::sync::Arc::new(std::sync::Mutex::new(
        ReadSession::for_zip_publication(65_536).unwrap(),
    ));
    let work = DecodeWork::shared(&session);
    assert!(matches!(
        select(&capture, &manifest, Some(&source), &outputs, work),
        Err(AppError::ManagementBudget)
    ));
    assert_eq!(
        session.lock().unwrap().begin_publication_attempt(),
        Err(ManagementError::Budget)
    );
    // A selector-first implementation would reject the bindings before reporting budget exhaustion.
    manifest.successful.clear();
    let session = std::sync::Arc::new(std::sync::Mutex::new(
        ReadSession::for_zip_publication(65_536).unwrap(),
    ));
    assert!(matches!(
        select(
            &capture,
            &manifest,
            Some(&source),
            &outputs,
            DecodeWork::shared(&session)
        ),
        Err(AppError::ManagementBudget)
    ));
}

#[test]
fn small_selection_matches_selector_and_charges_before_each_retry() {
    let (capture, source, outputs, manifest) = fixture(8, 2);
    let expected = select_zip_intents(&capture, &manifest, Some(&source), &outputs).unwrap();
    let session = std::sync::Arc::new(std::sync::Mutex::new(
        ReadSession::for_zip_publication(100).unwrap(),
    ));
    let work = DecodeWork::shared(&session);
    let (actual, resident) = select(&capture, &manifest, Some(&source), &outputs, work).unwrap();
    assert_eq!(actual, expected);
    let first = session.lock().unwrap().storage_usage().copied_bytes;
    assert!(first > 0);
    drop(actual);
    drop(resident);
    begin_attempt(&session).unwrap();
    let (_, _resident) = select(&capture, &manifest, Some(&source), &outputs, work).unwrap();
    assert_eq!(
        session.lock().unwrap().storage_usage().copied_bytes,
        first * 2
    );
}

#[test]
fn normal_four_psa_1649_selection_fits_unchanged_publication_limits() {
    let (capture, source, outputs, manifest) = fixture(16, 1649);
    let session = std::sync::Arc::new(std::sync::Mutex::new(
        ReadSession::for_zip_publication(65_536).unwrap(),
    ));
    let work = DecodeWork::shared(&session);
    let (candidate, candidate_resident) =
        select(&capture, &manifest, Some(&source), &outputs, work).unwrap();
    assert_eq!(candidate.leases.len(), 1649);
    let shape_bytes = work
        .with(|session| allocation::selection_shape_bytes(&candidate, session))
        .unwrap();
    let shape_resident = sql::retain_copy(work, shape_bytes).unwrap();
    let shape = allocation::selection_shape(&candidate);
    drop(candidate);
    drop(candidate_resident);
    let (exact, _resident) = select(&capture, &manifest, Some(&source), &outputs, work).unwrap();
    allocation::verify_candidate(&shape, &exact).unwrap();
    drop(shape_resident);
    assert!(session.lock().unwrap().storage_usage().copied_bytes < 256 * 1024 * 1024);
}

#[test]
fn management_default_limits_remain_enforced() {
    let defaults = Limits::default();
    assert_eq!(defaults.rows, 4096);
    assert_eq!(defaults.bytes, 16 * 1024 * 1024);
    assert_eq!(defaults.resident, 8 * 1024 * 1024);
    let mut rows = Budget::default();
    assert_eq!(rows.charge(0, 4097, 0, 0), Err(ManagementError::Budget));
    let mut resident = Budget::default();
    assert!(matches!(
        resident.reserve(8 * 1024 * 1024 + 1),
        Err(ManagementError::Budget)
    ));
    let mut session = ReadSession::default();
    assert_eq!(
        session.consume(0, 16 * 1024 * 1024 + 1),
        Err(ManagementError::Budget)
    );
}
