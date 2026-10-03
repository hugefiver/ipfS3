use super::*;
use crate::{
    config::{PinningConfig, PolicyConfig, ProviderConfig},
    pinning::{
        config::ValidatedPinningConfig,
        policy::{LeaseSource, PinPolicyEvaluator, PublicationContext},
        tags::ObjectTag,
    },
};

fn fixture() -> ValidatedPinningConfig {
    ValidatedPinningConfig::from_raw(
        &PinningConfig {
            providers: vec![ProviderConfig {
                name: "remote".into(),
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
            }],
            policies: vec![PolicyConfig {
                bucket: "photos".into(),
                prefix: "exports/".into(),
                trigger: "always".into(),
                provider_mode: "one".into(),
                providers: vec!["remote".into()],
                default_duration: "1h".into(),
                max_duration: "2h".into(),
                allow_decompressed: true,
            }],
            ..PinningConfig::default()
        },
        |_| None,
    )
    .unwrap()
}

fn rule(
    name: &str,
    prefix: &str,
    effect: ZipRuleEffect,
    policy_id: Option<String>,
) -> ZipOutputRuleConfig {
    ZipOutputRuleConfig {
        name: name.into(),
        priority: 10,
        bucket: "photos".into(),
        prefix: prefix.into(),
        effect,
        policy_id,
    }
}

fn output(key: &str, version: &str, cid: &str) -> ZipPublishedOutput {
    ZipPublishedOutput {
        bucket: "photos".into(),
        key: key.into(),
        version_id: version.into(),
        cid: cid.into(),
    }
}

fn rules(config: &ValidatedPinningConfig) -> Vec<ZipOutputRuleConfig> {
    vec![
        rule(
            "public",
            "exports/",
            ZipRuleEffect::Allow,
            Some(config.policies[0].identity.clone()),
        ),
        rule("private", "exports/private/", ZipRuleEffect::Deny, None),
    ]
}

#[test]
fn private_child_denies_even_with_broad_always_and_lower_priority() {
    let config = fixture();
    let engine = ValidatedZipOutputRules::compile(&rules(&config), &config).unwrap();
    let plan = engine
        .plan(
            ZipTargets {
                source: true,
                extracted: true,
            },
            Some(output("exports/archive.zip", "v1", "source-cid")),
            &[output("exports/private/secret", "v2", "private-cid")],
        )
        .unwrap();
    assert_eq!(plan.outputs[0].intents.len(), 1);
    assert!(plan.outputs[1].intents.is_empty());
    assert_eq!(plan.outputs[1].warning, Some(ZipPlanWarning::Denied));
}

#[test]
fn each_entry_matches_its_full_key_independently_and_same_cid_stays_independent() {
    let config = fixture();
    let engine = ValidatedZipOutputRules::compile(&rules(&config), &config).unwrap();
    let plan = engine
        .plan(
            ZipTargets {
                source: false,
                extracted: true,
            },
            Some(output("exports/archive.zip", "v1", "shared-cid")),
            &[
                output("exports/public/a", "v2", "shared-cid"),
                output("exports/private/b", "v3", "shared-cid"),
                output("exports/public/c", "v4", "shared-cid"),
            ],
        )
        .unwrap();
    assert_eq!(plan.outputs.len(), 3);
    assert!(
        plan.outputs
            .iter()
            .all(|entry| entry.kind == ZipOutputKind::Extracted)
    );
    assert!(
        plan.outputs[0]
            .intents
            .iter()
            .all(|i| i.intent.source == LeaseSource::Automatic)
    );
    assert!(plan.outputs[1].intents.is_empty());
    assert_eq!(plan.outputs[2].intents.len(), 1);
    assert_ne!(
        plan.outputs[0].output.version_id,
        plan.outputs[2].output.version_id
    );
    assert!(
        !plan
            .outputs
            .iter()
            .any(|item| item.output.key.ends_with(".zip"))
    );
}

#[test]
fn equal_priority_uses_name_not_input_order_and_prefix_is_literal() {
    let config = fixture();
    let mut candidates = rules(&config);
    candidates[1].prefix = "exports/priv*".into();
    candidates.push(rule(
        "aaa",
        "exports/",
        ZipRuleEffect::Allow,
        Some(config.policies[0].identity.clone()),
    ));
    let engine = ValidatedZipOutputRules::compile(&candidates, &config).unwrap();
    let plan = engine
        .plan(
            ZipTargets {
                source: false,
                extracted: true,
            },
            None,
            &[output("exports/private/x", "v1", "cid")],
        )
        .unwrap();
    assert_eq!(plan.outputs[0].rule_name.as_deref(), Some("aaa"));
    assert_eq!(plan.outputs[0].intents.len(), 1);
    candidates.reverse();
    let reordered = ValidatedZipOutputRules::compile(&candidates, &config).unwrap();
    assert_eq!(engine.revision(), reordered.revision());
    assert_eq!(
        plan,
        reordered
            .plan(
                ZipTargets {
                    source: false,
                    extracted: true
                },
                None,
                &[output("exports/private/x", "v1", "cid")]
            )
            .unwrap()
    );
}

#[test]
fn no_rule_or_disabled_entry_target_creates_no_intents() {
    let config = fixture();
    let engine = ValidatedZipOutputRules::compile(&rules(&config), &config).unwrap();
    let plan = engine
        .plan(
            ZipTargets {
                source: true,
                extracted: false,
            },
            Some(output("elsewhere/archive.zip", "v1", "cid")),
            &[output("exports/public/a", "v2", "cid")],
        )
        .unwrap();
    assert_eq!(plan.outputs.len(), 1);
    assert!(plan.outputs[0].intents.is_empty());
    assert_eq!(
        plan.outputs[0].warning,
        Some(ZipPlanWarning::NoMatchingRule)
    );
}

#[test]
fn no_remote_targets_does_not_block_locally_published_outputs() {
    let config = fixture();
    let engine = ValidatedZipOutputRules::compile(&rules(&config), &config).unwrap();
    let plan = engine
        .plan(
            ZipTargets {
                source: false,
                extracted: false,
            },
            None,
            &[output("exports/public/a", "v1", "cid")],
        )
        .unwrap();
    assert!(plan.outputs.is_empty());
}

#[test]
fn lower_priority_allow_wins_and_v2_deny_by_default_is_independent_of_publish_flags() {
    let config = fixture();
    let mut preferred = rule(
        "preferred",
        "exports/",
        ZipRuleEffect::Allow,
        Some(config.policies[0].identity.clone()),
    );
    preferred.priority = 0;
    let engine =
        ValidatedZipOutputRules::compile(&[rules(&config)[0].clone(), preferred], &config).unwrap();
    let plan = engine
        .plan(
            ZipTargets {
                source: true,
                extracted: false,
            },
            Some(output("exports/source.zip", "v1", "cid")),
            &[],
        )
        .unwrap();
    assert_eq!(plan.outputs[0].rule_name.as_deref(), Some("preferred"));

    let empty = ValidatedZipOutputRules::compile(&[], &config).unwrap();
    let plan = empty
        .plan(
            ZipTargets {
                source: true,
                extracted: false,
            },
            Some(output("exports/source.zip", "v1", "cid")),
            &[],
        )
        .unwrap();
    assert!(plan.outputs[0].intents.is_empty());
}

#[test]
fn rejects_policy_scope_widening_and_duplicate_output_versions() {
    let config = fixture();
    let id = config.policies[0].identity.clone();
    assert!(
        ValidatedZipOutputRules::compile(
            &[rule(
                "outside",
                "other/",
                ZipRuleEffect::Allow,
                Some(id.clone())
            )],
            &config
        )
        .is_err()
    );
    let mut other_bucket = rule("other-bucket", "exports/", ZipRuleEffect::Allow, Some(id));
    other_bucket.bucket = "another".into();
    assert!(ValidatedZipOutputRules::compile(&[other_bucket], &config).is_err());
    let engine = ValidatedZipOutputRules::compile(&rules(&config), &config).unwrap();
    assert!(
        engine
            .plan(
                ZipTargets {
                    source: false,
                    extracted: true
                },
                None,
                &[
                    output("exports/a", "v1", "cid"),
                    output("exports/a", "v2", "cid")
                ]
            )
            .is_err()
    );
}

#[test]
fn one_keeps_disabled_fallback_but_all_requires_every_provider_available() {
    let mut config = fixture();
    let mut disabled = config.providers[0].clone();
    disabled.name = "standby".into();
    disabled.identity =
        crate::pinning::identity::ProviderIdentity::legacy("standby", "noop", None, "noop", "cid");
    disabled.limits.priority = 2;
    disabled.limits.enabled = false;
    config
        .provider_limits
        .insert(disabled.name.clone(), disabled.limits.clone());
    config.providers.push(disabled);
    config.policies[0].providers.push("standby".into());
    config.policies[0].refresh_identity(0);
    let allow = rule(
        "allow",
        "exports/",
        ZipRuleEffect::Allow,
        Some(config.policies[0].identity.clone()),
    );
    let engine = ValidatedZipOutputRules::compile(&[allow], &config).unwrap();
    let plan = engine
        .plan(
            ZipTargets {
                source: true,
                extracted: false,
            },
            Some(output("exports/archive.zip", "v1", "cid")),
            &[],
        )
        .unwrap();
    assert_eq!(plan.outputs[0].intents[0].providers.len(), 2);
    assert!(!plan.outputs[0].intents[0].providers[1].enabled);

    config.policies[0].provider_mode = crate::pinning::config::ProviderMode::All;
    config.policies[0].refresh_identity(0);
    let allow = rule(
        "all",
        "exports/",
        ZipRuleEffect::Allow,
        Some(config.policies[0].identity.clone()),
    );
    assert!(ValidatedZipOutputRules::compile(&[allow], &config).is_err());
}

#[test]
fn real_remote_route_needs_explicit_identity_and_captures_safe_revision() {
    let raw: crate::config::Config = toml::from_str(
        r#"
        [[pinning.providers]]
        name = "remote"
        kind = "pinata"
        token_env = "PINATA_TOKEN"
        endpoint = "https://private.example.test"
        priority = 1
        max_bytes = 100
        max_pins = 10
        [[pinning.policies]]
        bucket = "photos"
        prefix = "exports/"
        trigger = "always"
        provider_mode = "one"
        providers = ["remote"]
        default_duration = "1h"
        max_duration = "2h"
    "#,
    )
    .unwrap();
    let legacy =
        ValidatedPinningConfig::from_config(&raw, |_| Some("private-token".into())).unwrap();
    let allow = rule(
        "allow",
        "exports/",
        ZipRuleEffect::Allow,
        Some(legacy.policies[0].identity.clone()),
    );
    assert!(ValidatedZipOutputRules::compile(std::slice::from_ref(&allow), &legacy).is_err());

    let mut explicit_raw = raw.clone();
    explicit_raw.pinning_identity = toml::from_str(
        r#"
        primary_storage_domain = "kubo:primary"
        [[providers]]
        config_name = "remote"
        provider_id = "pinata-prod"
        display_name = "Pinata"
        backend = "pinata"
        scope = "account:prod"
        storage_domain = "pinata:prod"
        credential_revision = 1
        endpoint_revision = 3
        secret_ref = "env:PINATA_TOKEN"
        api_profile = "pinata-v3"
        strategy = "cid"
    "#,
    )
    .unwrap();
    let explicit =
        ValidatedPinningConfig::from_config(&explicit_raw, |_| Some("private-token".into()))
            .unwrap();
    let engine = ValidatedZipOutputRules::compile(std::slice::from_ref(&allow), &explicit).unwrap();
    let plan = engine
        .plan(
            ZipTargets {
                source: true,
                extracted: false,
            },
            Some(output("exports/archive.zip", "v1", "cid")),
            &[],
        )
        .unwrap();
    let route = &plan.outputs[0].intents[0].providers[0].route;
    assert_eq!(route.provider_id, "pinata-prod");
    assert_eq!(route.endpoint_revision, 3);
    let json = serde_json::to_string(&plan).unwrap();
    assert!(!json.contains("private-token"));
    assert!(!json.contains("private.example"));
    assert!(!json.contains("https://"));

    explicit_raw.pinning_identity.providers[0].endpoint_revision = 4;
    let rotated =
        ValidatedPinningConfig::from_config(&explicit_raw, |_| Some("private-token".into()))
            .unwrap();
    assert_ne!(
        engine.revision(),
        ValidatedZipOutputRules::compile(&[allow], &rotated)
            .unwrap()
            .revision()
    );
}

#[test]
fn rejects_unknown_policy_and_never_emits_secret_or_endpoint() {
    let config = fixture();
    assert!(
        ValidatedZipOutputRules::compile(
            &[rule(
                "bad",
                "exports/",
                ZipRuleEffect::Allow,
                Some("missing".into())
            )],
            &config
        )
        .is_err()
    );
    let engine = ValidatedZipOutputRules::compile(&rules(&config), &config).unwrap();
    let plan = engine
        .plan(
            ZipTargets {
                source: true,
                extracted: false,
            },
            Some(output("exports/archive.zip", "v1", "cid")),
            &[],
        )
        .unwrap();
    let serialized = serde_json::to_string(&plan).unwrap();
    assert!(serialized.contains("version_id"));
    assert!(serialized.contains("provider_id"));
    assert!(!serialized.contains("\"endpoint\":"));
    assert!(!serialized.contains("https://"));
    assert!(!serialized.contains("token"));
    assert_eq!(
        serde_json::from_str::<ZipOutputPinPlan>(&serialized).unwrap(),
        plan
    );
}

#[test]
fn v2_rules_do_not_change_legacy_first_match_or_inherit_archive_manual_control() {
    let mut config = fixture();
    let mut broad = config.policies[0].clone();
    broad.trigger = super::PolicyTrigger::Request;
    broad.refresh_identity(0);
    config.policies.insert(0, broad);
    config.policies[1].refresh_identity(1);

    let evaluator = PinPolicyEvaluator::new(&config);
    let archive_tags = [ObjectTag::new("ipfs-s3:pin", "true")];
    let legacy = evaluator
        .evaluate_publication(PublicationContext {
            bucket: "photos",
            key: "exports/archive.zip",
            tags: &archive_tags,
            is_decompress_zip: true,
        })
        .unwrap();
    assert_eq!(legacy.leases.len(), 1);
    assert_eq!(legacy.leases[0].source, LeaseSource::Manual);
    assert_eq!(legacy.leases[0].policy_id, config.policies[0].identity);

    let engine = ValidatedZipOutputRules::compile(
        &[rule(
            "entry",
            "exports/",
            ZipRuleEffect::Allow,
            Some(config.policies[0].identity.clone()),
        )],
        &config,
    )
    .unwrap();
    let plan = engine
        .plan(
            ZipTargets {
                source: false,
                extracted: true,
            },
            Some(output("exports/archive.zip", "v1", "cid")),
            &[output("exports/item", "v2", "cid")],
        )
        .unwrap();
    assert!(plan.outputs[0].intents.is_empty());
    assert_eq!(plan.outputs[0].output.key, "exports/item");
}

#[test]
fn valid_config_is_opt_in_and_unknown_fields_cannot_supply_endpoints() {
    let config: crate::config::Config = toml::from_str(
        "\
        [[decompress_zip.pin_output_rules]]\n\
        name = 'private'\n\
        priority = 0\n\
        bucket = '*'\n\
        prefix = 'private/'\n\
        effect = 'deny'\n",
    )
    .unwrap();
    assert_eq!(config.decompress_zip.pin_output_rules.len(), 1);
    assert!(matches!(
        config.decompress_zip.pin_output_rules[0].effect,
        ZipRuleEffect::Deny
    ));
    let absent: crate::config::Config = toml::from_str("").unwrap();
    assert!(absent.decompress_zip.pin_output_rules.is_empty());
    assert!(
        toml::from_str::<crate::config::Config>(
            "\
        [[decompress_zip.pin_output_rules]]\n\
        name = 'private'\n\
        priority = 0\n\
        bucket = '*'\n\
        effect = 'deny'\n\
        endpoint = 'https://host.example'\n"
        )
        .is_err()
    );
}
