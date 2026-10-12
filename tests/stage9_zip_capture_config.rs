use http::HeaderMap;
use ipfs_s3_gateway::{
    config::{Config, DecompressZipConfig, OptionalPinControlMode, PinningConfig},
    pinning::{
        config::ValidatedPinningConfig,
        coordinator::PinningCoordinator,
        decision::DecisionOrigin,
        zip_policy::{ZipCaptureContext, ZipOutputRuleConfig, ZipRuleEffect, capture_zip_intent},
    },
    state::AppState,
    zip::{extract::ZipExtractionLimits, options::parse_authenticated_zip_controls},
};

const BASE: &str = r#"
    [storage]
    database_url = "sqlite::memory:"
    [pinning]
    [[pinning.providers]]
    name = "noop"
    kind = "noop"
    priority = 1
    max_bytes = 100
    max_pins = 10
    [[pinning.policies]]
    bucket = "bucket"
    prefix = "outputs/"
    trigger = "always"
    provider_mode = "one"
    providers = ["noop"]
    default_duration = "1h"
    max_duration = "2h"
    allow_decompressed = true
    [decompress_zip]
    unixfs_directory_root = false
    max_decompressed_bytes = 23
    max_single_entry_bytes = 11
    max_entries = 2
    max_metadata_bytes = 4096
    max_staged_adds = 1
    processing_deadline_secs = 3
"#;

const DENY_RULE: &str = r#"
    [[decompress_zip.pin_output_rules]]
    name = "deny"
    priority = 1
    bucket = "bucket"
    prefix = "outputs/"
    effect = "deny"
"#;

fn assert_config(actual: &DecompressZipConfig, expected: &DecompressZipConfig) {
    assert_eq!(actual.unixfs_directory_root, expected.unixfs_directory_root);
    assert_eq!(actual.limits, expected.limits);
    assert_eq!(actual.auto_pin_outputs, expected.auto_pin_outputs);
    assert_eq!(actual.pin_output_rules, expected.pin_output_rules);
    assert_eq!(
        actual.pin_output_rules_provided,
        expected.pin_output_rules_provided
    );
    assert_eq!(actual.output_rules(), expected.output_rules());
}

#[tokio::test]
async fn real_toml_reaches_production_coordinator_and_capture_without_losing_presence() {
    for (fields, auto, rules_provided, extracted) in [
        ("", None, false, false),
        ("auto_pin_outputs = false", Some(false), false, false),
        ("auto_pin_outputs = true", Some(true), false, true),
        (
            "auto_pin_outputs = false\npin_output_rules = []",
            Some(false),
            true,
            false,
        ),
        (
            "auto_pin_outputs = true\npin_output_rules = []",
            Some(true),
            true,
            false,
        ),
    ] {
        let mut config: Config = toml::from_str(&format!("{BASE}\n{fields}")).unwrap();
        assert_eq!(config.decompress_zip.auto_pin_outputs, auto);
        assert_eq!(
            config.decompress_zip.pin_output_rules_provided,
            rules_provided
        );
        let frozen = config.decompress_zip.clone();
        let state = AppState::new(&config).await.unwrap();
        assert_config(state.pinning.decompress_zip_config(), &frozen);
        assert!(!state.pinning.zip_root_default());
        assert_eq!(state.pinning.zip_extraction_limits(), frozen.limits);

        let parsed = parse_authenticated_zip_controls(
            &HeaderMap::new(),
            &[("decompress-zip".into(), "outputs/".into())],
            &[],
            true,
            state.pinning.zip_root_default(),
        )
        .unwrap()
        .unwrap();
        let capture = capture_zip_intent(
            state.pinning.effective_config(),
            state.pinning.decompress_zip_config(),
            state.pinning.control_mode(),
            ZipCaptureContext {
                bucket: "bucket",
                source_key: "archive.zip",
                target_prefix: "outputs/",
                tags: &[],
            },
            &parsed,
            DecisionOrigin::new("principal", "request"),
        )
        .unwrap();
        assert_eq!(capture.controls().auto_pin_outputs, auto.unwrap_or(false));
        assert_eq!(capture.controls().targets.extracted, extracted);

        config.decompress_zip.unixfs_directory_root = true;
        config.decompress_zip.auto_pin_outputs = Some(!auto.unwrap_or(false));
        config.decompress_zip.pin_output_rules_provided = !rules_provided;
        config.decompress_zip.limits = ZipExtractionLimits::default();
        assert_config(state.pinning.decompress_zip_config(), &frozen);
    }

    let mut config: Config =
        toml::from_str(&format!("{BASE}\nauto_pin_outputs = true\n{DENY_RULE}")).unwrap();
    let frozen = config.decompress_zip.clone();
    let validated = ValidatedPinningConfig::from_config(&config, |_| None).unwrap();
    let coordinator = PinningCoordinator::build_with_decompress_config(
        validated,
        None,
        OptionalPinControlMode::Warn,
        &config.decompress_zip,
    )
    .unwrap();
    assert_eq!(coordinator.control_mode(), OptionalPinControlMode::Warn);
    assert_config(coordinator.decompress_zip_config(), &frozen);
    config.decompress_zip.pin_output_rules.clear();
    assert_config(coordinator.decompress_zip_config(), &frozen);
}

#[tokio::test]
async fn omitted_auto_with_supplied_rules_still_requires_a_migration_choice() {
    for rules in ["pin_output_rules = []", DENY_RULE] {
        let config: Config = toml::from_str(&format!("{BASE}\n{rules}")).unwrap();
        assert_eq!(config.decompress_zip.auto_pin_outputs, None);
        assert!(config.decompress_zip.pin_output_rules_provided);
        let error = match AppState::new(&config).await {
            Ok(_) => panic!("omitted ZIP migration choice must block startup"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("ZIP configuration migration required")
        );
    }
}

#[test]
fn legacy_builders_keep_defaults_and_original_root_rules_and_limits() {
    let validated = ValidatedPinningConfig::from_raw(&PinningConfig::default(), |_| None).unwrap();
    let rule = ZipOutputRuleConfig {
        name: "deny".into(),
        priority: 1,
        bucket: "bucket".into(),
        prefix: "outputs/".into(),
        effect: ZipRuleEffect::Deny,
        policy_id: None,
    };
    let configured: Config = toml::from_str(BASE).unwrap();
    let limits = configured.decompress_zip.limits;
    let defaults = DecompressZipConfig::default();
    let mut root_off = defaults.clone();
    root_off.unixfs_directory_root = false;
    let mut with_rules = root_off.clone();
    with_rules.pin_output_rules.push(rule.clone());
    let mut with_limits = root_off.clone();
    with_limits.limits = limits;
    for (coordinator, expected) in [
        (
            PinningCoordinator::build(validated.clone()).unwrap(),
            defaults.clone(),
        ),
        (
            PinningCoordinator::build_with_kubo(validated.clone(), None).unwrap(),
            defaults.clone(),
        ),
        (
            PinningCoordinator::build_with_kubo_and_mode(
                validated.clone(),
                None,
                OptionalPinControlMode::Warn,
            )
            .unwrap(),
            defaults,
        ),
        (
            PinningCoordinator::build_with_kubo_mode_and_zip_root(
                validated.clone(),
                None,
                OptionalPinControlMode::Strict,
                false,
            )
            .unwrap(),
            root_off,
        ),
        (
            PinningCoordinator::build_with_kubo_mode_and_zip_root_and_rules(
                validated.clone(),
                None,
                OptionalPinControlMode::Strict,
                false,
                &[rule],
            )
            .unwrap(),
            with_rules,
        ),
        (
            PinningCoordinator::build_with_zip_limits(
                validated,
                None,
                OptionalPinControlMode::Strict,
                false,
                &[],
                limits,
            )
            .unwrap(),
            with_limits,
        ),
    ] {
        assert_config(coordinator.decompress_zip_config(), &expected);
        assert_eq!(
            coordinator.zip_root_default(),
            expected.unixfs_directory_root
        );
        assert_eq!(coordinator.zip_extraction_limits(), expected.limits);
    }
}
