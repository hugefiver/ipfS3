use http::{HeaderMap, HeaderValue};

use super::{fixture, rule, rules};
use crate::{
    config::{Config, DecompressZipConfig, OptionalPinControlMode},
    pinning::{
        config::{PolicyTrigger, ValidatedPinningConfig},
        decision::DecisionOrigin,
        policy::LeaseSource,
        tags::ObjectTag,
        zip_policy::*,
    },
    zip::{
        batch::{FinalZipManifest, ManifestFailure, ManifestFile},
        options::parse_authenticated_zip_controls,
    },
};

fn bound(key: &str, id: &str) -> ZipBoundOutput {
    ZipBoundOutput {
        object_id: id.into(),
        bucket: "photos".into(),
        key: key.into(),
        version_id: "null".into(),
        cid: "shared-cid".into(),
        logical_size: 5,
    }
}

fn manifest(outputs: &[ZipBoundOutput]) -> FinalZipManifest {
    FinalZipManifest {
        successful: outputs
            .iter()
            .map(|output| ManifestFile {
                relative_path: output.key.trim_start_matches("exports/").into(),
                object_key: output.key.clone(),
                cid: output.cid.clone(),
                size: output.logical_size,
            })
            .collect(),
        failed: vec![ManifestFailure {
            path: "failed/bad".into(),
            code: "entry_failed",
        }],
        root_error: Some("file_directory_conflict"),
    }
}

fn capture(
    config: &ValidatedPinningConfig,
    zip: &DecompressZipConfig,
    v2: bool,
    extras: &[(&str, &str)],
    tags: &[ObjectTag],
) -> anyhow::Result<CapturedZipIntent> {
    let mut headers = HeaderMap::new();
    if v2 {
        headers.insert("x-ipfs3-zip-contract", HeaderValue::from_static("v2"));
    }
    for (name, value) in extras {
        headers.insert(
            http::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    let controls = parse_authenticated_zip_controls(
        &headers,
        &[("decompress-zip".into(), "exports/".into())],
        tags,
        true,
        true,
    )?
    .unwrap();
    capture_zip_intent(
        config,
        zip,
        OptionalPinControlMode::Warn,
        ZipCaptureContext {
            bucket: "photos",
            source_key: "exports/archive.zip",
            target_prefix: "exports/",
            tags,
        },
        &controls,
        DecisionOrigin::new("tenant", "admission"),
    )
}

fn enabled() -> DecompressZipConfig {
    DecompressZipConfig {
        auto_pin_outputs: Some(true),
        ..Default::default()
    }
}

#[test]
fn r25_same_policy_v1_v2_default_is_per_exact_output_and_selection_is_pure() {
    let config = fixture();
    let source = bound("exports/archive.zip", "source");
    let outputs = [bound("exports/a", "first"), bound("exports/b", "second")];
    let final_manifest = manifest(&outputs);
    for v2 in [false, true] {
        let captured = capture(&config, &enabled(), v2, &[], &[]).unwrap();
        assert!(!captured.controls().targets.source);
        assert!(captured.source_decision().effective_intents.is_empty());
        let selected =
            select_zip_intents(&captured, &final_manifest, Some(&source), &outputs).unwrap();
        assert_eq!(
            selected,
            select_zip_intents(&captured, &final_manifest, Some(&source), &outputs).unwrap()
        );
        assert_eq!(selected.leases.len(), 2);
        for (lease, output) in selected.leases.iter().zip(&outputs) {
            assert_eq!(&lease.owner, output);
            assert_eq!(lease.targets, vec![output.clone()]);
            assert_eq!(lease.intent.source, LeaseSource::Automatic);
            assert_eq!(lease.intent.duration, config.policies[0].default_duration);
        }
        assert_ne!(
            selected.leases[0].owner.object_id,
            selected.leases[1].owner.object_id
        );
        assert_eq!(selected.leases[0].owner.cid, selected.leases[1].owner.cid);
        assert_eq!(selected.output_decisions.len(), 2);
        if v2 {
            assert_eq!(
                captured.v2_options().unwrap().targets,
                crate::zip::options::ZipTargets::Extracted
            );
        }
    }
}

#[test]
fn r25_no_provider_request_only_and_false_cannot_create_default_work() {
    let absent =
        ValidatedPinningConfig::from_raw(&crate::config::PinningConfig::default(), |_| None)
            .unwrap();
    let mut request_only = fixture();
    request_only.policies[0].trigger = PolicyTrigger::Request;
    request_only.policies[0].refresh_identity(0);
    let source = bound("exports/archive.zip", "source");
    let outputs = [bound("exports/a", "output")];
    for config in [&absent, &request_only] {
        for flag in [None, Some(true), Some(false)] {
            for v2 in [false, true] {
                let zip = DecompressZipConfig {
                    auto_pin_outputs: flag,
                    ..Default::default()
                };
                let captured = capture(config, &zip, v2, &[], &[]).unwrap();
                assert!(
                    !captured.controls().targets.source && !captured.controls().targets.extracted
                );
                assert!(
                    select_zip_intents(&captured, &manifest(&outputs), Some(&source), &outputs)
                        .unwrap()
                        .leases
                        .is_empty()
                );
            }
        }
    }
    let off = DecompressZipConfig {
        auto_pin_outputs: Some(false),
        ..Default::default()
    };
    assert!(
        select_zip_intents(
            &capture(&fixture(), &off, false, &[], &[]).unwrap(),
            &manifest(&outputs),
            Some(&source),
            &outputs
        )
        .unwrap()
        .leases
        .is_empty()
    );
}

#[test]
fn r25_manual_decompressed_stays_real_archive_group_and_empty_skips_lease() {
    let config = fixture();
    let tags = [
        ObjectTag::new("ipfs-s3:pin", "true"),
        ObjectTag::new("ipfs-s3:content", "decompressed"),
    ];
    let zip = DecompressZipConfig {
        auto_pin_outputs: Some(false),
        ..Default::default()
    };
    let source = bound("exports/archive.zip", "source");
    let outputs = [bound("exports/a", "output")];
    for v2 in [false, true] {
        let captured = capture(&config, &zip, v2, &[], &tags).unwrap();
        let selected =
            select_zip_intents(&captured, &manifest(&outputs), Some(&source), &outputs).unwrap();
        assert_eq!(selected.leases.len(), 1);
        assert_eq!(selected.leases[0].intent.source, LeaseSource::Manual);
        assert_eq!(selected.leases[0].owner, source);
        assert_eq!(selected.leases[0].targets, outputs);
        assert!(
            select_zip_intents(&captured, &FinalZipManifest::default(), Some(&source), &[])
                .unwrap()
                .leases
                .is_empty()
        );
        let both = capture(&config, &enabled(), v2, &[], &tags).unwrap();
        assert_eq!(
            select_zip_intents(&both, &manifest(&outputs), Some(&source), &outputs)
                .unwrap()
                .leases
                .len(),
            2
        );
    }
}

#[test]
fn r25_source_false_is_ownerless_and_explicit_source_is_not_disabled_by_auto_switch() {
    let config = fixture();
    let outputs = [bound("exports/a", "output")];
    let source = bound("exports/archive.zip", "source");
    let no_source = capture(
        &config,
        &enabled(),
        true,
        &[
            ("x-ipfs3-zip-publish-source", "false"),
            ("x-ipfs3-zip-token", "retry"),
        ],
        &[],
    )
    .unwrap();
    let selected = select_zip_intents(&no_source, &manifest(&outputs), None, &outputs).unwrap();
    assert_eq!(selected.leases[0].owner, outputs[0]);
    assert!(selected.source_decision.is_none());
    assert!(select_zip_intents(&no_source, &manifest(&outputs), Some(&source), &outputs).is_err());
    assert!(
        capture(
            &config,
            &enabled(),
            true,
            &[
                ("x-ipfs3-zip-publish-source", "false"),
                ("x-ipfs3-zip-token", "retry")
            ],
            &[ObjectTag::new("ipfs-s3:pin", "true")]
        )
        .is_err()
    );
    let off = DecompressZipConfig {
        auto_pin_outputs: Some(false),
        ..Default::default()
    };
    for (targets, expected) in [("none", 0), ("source", 1), ("extracted", 1), ("both", 2)] {
        let explicit = capture(
            &config,
            &off,
            true,
            &[("x-ipfs3-zip-targets", targets)],
            &[],
        )
        .unwrap();
        let selected =
            select_zip_intents(&explicit, &manifest(&outputs), Some(&source), &outputs).unwrap();
        assert_eq!(selected.leases.len(), expected);
        assert_eq!(
            selected.source_decision.unwrap().effective_intents.len(),
            usize::from(matches!(targets, "source" | "both"))
        );
    }
}

#[test]
fn r25_manifest_binding_rejects_nonfinal_root_directory_failed_and_wrong_versions() {
    let source = bound("exports/archive.zip", "source");
    let outputs = [bound("exports/a", "output")];
    let captured = capture(&fixture(), &enabled(), false, &[], &[]).unwrap();
    let final_manifest = manifest(&outputs);
    for bad in [
        bound("exports/", "root"),
        bound("exports/dir/", "directory"),
        bound("exports/failed", "failed"),
        bound("exports/archive.zip", "source"),
        ZipBoundOutput {
            cid: "wrong-cid".into(),
            ..outputs[0].clone()
        },
        ZipBoundOutput {
            logical_size: 6,
            ..outputs[0].clone()
        },
        ZipBoundOutput {
            object_id: "".into(),
            ..outputs[0].clone()
        },
    ] {
        assert!(select_zip_intents(&captured, &final_manifest, Some(&source), &[bad]).is_err());
    }
    assert!(select_zip_intents(&captured, &final_manifest, Some(&source), &[]).is_err());
    assert!(
        select_zip_intents(
            &captured,
            &final_manifest,
            Some(&source),
            &[outputs[0].clone(), outputs[0].clone()]
        )
        .is_err()
    );
    assert!(
        select_zip_intents(&captured, &FinalZipManifest::default(), Some(&source), &[])
            .unwrap()
            .leases
            .is_empty()
    );
}

#[test]
fn r25_common_rules_omitted_vs_empty_and_deny_veto_apply_to_both_contracts() {
    let config = fixture();
    let source = bound("exports/archive.zip", "source");
    let outputs = [
        bound("exports/public", "public"),
        bound("exports/private/x", "private"),
    ];
    for v2 in [false, true] {
        let mut zip = enabled();
        zip.pin_output_rules = rules(&config);
        let captured = capture(&config, &zip, v2, &[], &[]).unwrap();
        let selected =
            select_zip_intents(&captured, &manifest(&outputs), Some(&source), &outputs).unwrap();
        assert_eq!(selected.leases.len(), 1);
        assert_eq!(selected.warnings[0].1, ZipPlanWarning::Denied);
        zip.pin_output_rules.clear();
        zip.pin_output_rules_provided = true;
        let empty = capture(&config, &zip, v2, &[], &[]).unwrap();
        assert!(!empty.controls().targets.extracted);
        assert!(
            select_zip_intents(&empty, &manifest(&outputs), Some(&source), &outputs)
                .unwrap()
                .leases
                .is_empty()
        );
    }
}

#[test]
fn r25_policy_index_resolves_exact_alias_at_load_and_bad_references_reject() {
    let raw: Config =
        toml::from_str("[decompress_zip]\nauto_pin_outputs=true\npin_output_rules=[]\n").unwrap();
    assert_eq!(raw.decompress_zip.output_rules(), Some([].as_slice()));
    assert!(
        Config::default_for_test()
            .decompress_zip
            .output_rules()
            .is_none()
    );
    let config = fixture();
    let indexed: ZipOutputRuleConfig = toml::from_str("name='readable'\npriority=0\nbucket='photos'\nprefix='exports/'\neffect='allow'\npolicy_index=0").unwrap();
    let mut alias = rule(
        "readable",
        "exports/",
        ZipRuleEffect::Allow,
        Some(config.policies[0].identity.clone()),
    );
    alias.priority = 0;
    assert_eq!(
        ValidatedZipOutputRules::compile(&[indexed], &config).unwrap(),
        ValidatedZipOutputRules::compile(&[alias], &config).unwrap()
    );
    for suffix in [
        "policy_index=99",
        "policy_id='missing'",
        "policy_index=0\npolicy_id='missing'",
    ] {
        let raw = format!(
            "name='bad'\npriority=0\nbucket='photos'\nprefix='exports/'\neffect='allow'\n{suffix}"
        );
        assert!(
            toml::from_str::<ZipOutputRuleConfig>(&raw)
                .ok()
                .is_none_or(|rule| ValidatedZipOutputRules::compile(&[rule], &config).is_err())
        );
    }
    let legacy_rules: Config = toml::from_str("[decompress_zip]\npin_output_rules=[]").unwrap();
    assert!(
        ValidatedPinningConfig::from_config(&legacy_rules, |_| None)
            .unwrap_err()
            .to_string()
            .contains("migration")
    );
}

#[test]
fn r25_captured_policy_and_skipped_decision_are_not_reinterpreted_by_current_config() {
    let mut config = fixture();
    let source = bound("exports/archive.zip", "source");
    let outputs = [bound("exports/a", "output")];
    let captured = capture(&config, &enabled(), true, &[], &[]).unwrap();
    let frozen = serde_json::to_string(&captured).unwrap();
    let expected =
        select_zip_intents(&captured, &manifest(&outputs), Some(&source), &outputs).unwrap();
    config.policies.clear();
    config.providers[0].limits.enabled = false;
    let decoded = CapturedZipIntent::decode_json(&frozen).unwrap();
    assert_eq!(
        select_zip_intents(&decoded, &manifest(&outputs), Some(&source), &outputs).unwrap(),
        expected
    );
    assert!(
        !capture(&config, &enabled(), true, &[], &[])
            .unwrap()
            .controls()
            .targets
            .extracted
    );
    let skipped = capture(
        &config,
        &enabled(),
        false,
        &[],
        &[ObjectTag::new("ipfs-s3:pin", "true")],
    )
    .unwrap();
    assert_eq!(
        skipped.source_decision().effect,
        crate::pinning::decision::DecisionEffect::Skipped
    );
    let current = fixture();
    assert_eq!(current.policies.len(), 1);
    let retained =
        CapturedZipIntent::decode_json(&serde_json::to_string(&skipped).unwrap()).unwrap();
    assert_eq!(
        retained.source_decision().effect,
        crate::pinning::decision::DecisionEffect::Skipped
    );
    assert!(
        select_zip_intents(&retained, &manifest(&outputs), Some(&source), &outputs)
            .unwrap()
            .leases
            .is_empty()
    );
    assert!(
        CapturedZipIntent::decode_json(
            &frozen.replace("\"schema_version\":1", "\"schema_version\":99")
        )
        .is_err()
    );
    assert!(
        decode_legacy_v2_intent(
            "{\"options\":{},\"target_prefix\":\"exports/\",\"rule_revision\":\"old\"}"
        )
        .unwrap_err()
        .to_string()
        .contains("needs_attention")
    );
}

#[test]
fn r25_stage6_confirmation_is_frozen_in_output_and_manual_routes() {
    let mut config = fixture();
    config.providers[0].cluster = Some(crate::pinning::cluster::provider::ClusterProviderOptions {
        min_replicas: 3,
        ..Default::default()
    });
    let required = config.providers[0].route_snapshot().confirmation;
    assert!(required.is_some());
    let captured = capture(
        &config,
        &enabled(),
        false,
        &[],
        &[ObjectTag::new("ipfs-s3:pin", "true")],
    )
    .unwrap();
    let source = bound("exports/archive.zip", "source");
    let outputs = [bound("exports/a", "output")];
    let selected =
        select_zip_intents(&captured, &manifest(&outputs), Some(&source), &outputs).unwrap();
    assert_eq!(selected.leases.len(), 2);
    for lease in selected.leases {
        assert_eq!(lease.providers[0].route.confirmation, required);
    }
    let json = serde_json::to_string(&captured).unwrap();
    assert_eq!(CapturedZipIntent::decode_json(&json).unwrap(), captured);
}

#[test]
fn r25_legacy_v1_frozen_archive_auto_and_manual_owner_are_preserved() {
    let config = fixture();
    let tags = [
        ObjectTag::new("ipfs-s3:pin", "true"),
        ObjectTag::new("ipfs-s3:content", "decompressed"),
    ];
    let captured = capture(&config, &enabled(), false, &[], &tags).unwrap();
    let (_, old_decision) = crate::pinning::policy::PinPolicyEvaluator::with_mode(
        &config,
        OptionalPinControlMode::Warn,
    )
    .evaluate_publication_decision(
        crate::pinning::policy::PublicationContext {
            bucket: "photos",
            key: "exports/archive.zip",
            tags: &tags,
            is_decompress_zip: true,
        },
        DecisionOrigin::new("tenant", "admission"),
    )
    .unwrap();
    let value = serde_json::to_value(&captured).unwrap();
    let historical = serde_json::json!({
        "bucket": "photos", "source_key": "exports/archive.zip", "target_prefix": "exports/",
        "root_capture": value["controls"]["root"], "result_enabled": false,
        "decision": old_decision, "providers": value["source_providers"],
    });
    let decoded = decode_legacy_v1_intent(&historical.to_string()).unwrap();
    assert!(!decoded.controls().result_enabled);
    let source = bound("exports/archive.zip", "source");
    let outputs = [bound("exports/a", "output")];
    let selected =
        select_zip_intents(&decoded, &manifest(&outputs), Some(&source), &outputs).unwrap();
    assert_eq!(selected.leases.len(), 2);
    assert!(selected.leases.iter().all(|lease| lease.owner == source));
    assert_eq!(selected.leases[0].intent.source, LeaseSource::Automatic);
    assert_eq!(
        selected.leases[0].targets.as_slice(),
        std::slice::from_ref(&source)
    );
    assert_eq!(selected.leases[1].intent.source, LeaseSource::Manual);
    assert_eq!(selected.leases[1].targets, outputs);
}

#[test]
fn r25_ordered_first_match_cannot_promote_shadowed_request_policy_to_always() {
    let mut config = fixture();
    let always = config.policies[0].clone();
    config.policies[0].trigger = PolicyTrigger::Request;
    config.policies[0].refresh_identity(0);
    config.policies.push(always);
    config.policies[1].refresh_identity(1);
    let source = bound("exports/archive.zip", "source");
    let outputs = [bound("exports/a", "output")];
    for v2 in [false, true] {
        let captured = capture(&config, &enabled(), v2, &[], &[]).unwrap();
        assert!(!captured.controls().targets.extracted);
        assert!(
            select_zip_intents(&captured, &manifest(&outputs), Some(&source), &outputs)
                .unwrap()
                .leases
                .is_empty()
        );
    }
}

#[test]
fn r25_legacy_v2_exact_frozen_plan_survives_config_change_but_not_evidence_loss() {
    let mut config = fixture();
    let captured = capture(&config, &enabled(), true, &[], &[]).unwrap();
    let value = serde_json::to_value(&captured).unwrap();
    let old = ValidatedZipOutputRules::compile(&rules(&config), &config)
        .unwrap()
        .plan(
            ZipTargets {
                source: false,
                extracted: true,
            },
            None,
            &[super::output("exports/a", "null", "shared-cid")],
        )
        .unwrap();
    let mut historical = serde_json::json!({
        "bucket": "photos", "source_key": "exports/archive.zip", "target_prefix": "exports/",
        "options": captured.v2_options(), "source_decision": value["source_decision"],
        "providers": value["source_providers"], "pin_plan": old,
    });
    let decoded = decode_legacy_v2_intent(&historical.to_string()).unwrap();
    config.policies.clear();
    let source = bound("exports/archive.zip", "source");
    let outputs = [bound("exports/a", "output")];
    let selected =
        select_zip_intents(&decoded, &manifest(&outputs), Some(&source), &outputs).unwrap();
    assert_eq!(selected.leases.len(), 1);
    assert_eq!(selected.leases[0].owner, outputs[0]);
    assert!(
        select_zip_intents(&decoded, &FinalZipManifest::default(), Some(&source), &[]).is_err()
    );
    historical["pin_plan"]["outputs"][0]["intents"][0]["providers"][0]["route"]["api_profile"] =
        "cluster-rest".into();
    assert!(
        decode_legacy_v2_intent(&historical.to_string())
            .unwrap_err()
            .to_string()
            .contains("confirmation")
    );
}

#[test]
fn r25_empty_compatibility_revision_stays_legacy_but_current_capture_binds_policies() {
    let configured = fixture();
    let disabled =
        ValidatedPinningConfig::from_raw(&crate::config::PinningConfig::default(), |_| None)
            .unwrap();
    let source = bound("exports/archive.zip", "source");
    let outputs = [bound("exports/a", "output")];
    let published = [super::output("exports/a", "null", "shared-cid")];
    for config in [&configured, &disabled] {
        let compatibility = ValidatedZipOutputRules::compile(&[], config).unwrap();
        // Committed old encoder: domain bytes followed by a zero u64 count.
        assert_eq!(
            compatibility.revision(),
            "5c85eb380c83a82f29bb4ec40150eaf3cee5628d2fdb67b9421bba7379dc8634"
        );
        let plan = compatibility
            .plan(
                ZipTargets {
                    source: false,
                    extracted: true,
                },
                None,
                &published,
            )
            .unwrap();
        assert_eq!(
            plan.outputs[0].warning,
            Some(ZipPlanWarning::NoMatchingRule)
        );
        assert!(plan.outputs[0].intents.is_empty());
    }
    let active_capture = capture(&configured, &enabled(), true, &[], &[]).unwrap();
    let inactive_capture = capture(&disabled, &enabled(), true, &[], &[]).unwrap();
    assert_ne!(
        active_capture.policy_revision(),
        inactive_capture.policy_revision()
    );
    for (captured, expected_leases) in [(&active_capture, 1), (&inactive_capture, 0)] {
        assert_eq!(
            select_zip_intents(captured, &manifest(&outputs), Some(&source), &outputs)
                .unwrap()
                .leases
                .len(),
            expected_leases
        );
    }
}

#[test]
fn r25_legacy_empty_rule_plan_stays_no_intent_when_current_auto_is_enabled() {
    let config = fixture();
    let current = capture(&config, &enabled(), true, &[], &[]).unwrap();
    let value = serde_json::to_value(&current).unwrap();
    let old_plan = ValidatedZipOutputRules::compile(&[], &config)
        .unwrap()
        .plan(
            ZipTargets {
                source: false,
                extracted: true,
            },
            None,
            &[super::output("exports/a", "null", "shared-cid")],
        )
        .unwrap();
    assert_eq!(
        old_plan.rule_revision,
        "5c85eb380c83a82f29bb4ec40150eaf3cee5628d2fdb67b9421bba7379dc8634"
    );
    let historical = serde_json::json!({
        "bucket": "photos", "source_key": "exports/archive.zip", "target_prefix": "exports/",
        "options": current.v2_options(), "source_decision": value["source_decision"],
        "providers": value["source_providers"], "pin_plan": old_plan,
    });
    let decoded = decode_legacy_v2_intent(&historical.to_string()).unwrap();
    let source = bound("exports/archive.zip", "source");
    let outputs = [bound("exports/a", "output")];
    let final_manifest = manifest(&outputs);
    let previous = select_zip_intents(&decoded, &final_manifest, Some(&source), &outputs).unwrap();
    assert!(previous.leases.is_empty());
    assert!(previous.output_decisions[0].1.effective_intents.is_empty());
    assert_eq!(previous.warnings[0].1, ZipPlanWarning::NoMatchingRule);
    assert_eq!(
        select_zip_intents(&current, &final_manifest, Some(&source), &outputs)
            .unwrap()
            .leases
            .len(),
        1
    );
    assert_eq!(
        select_zip_intents(&decoded, &final_manifest, Some(&source), &outputs).unwrap(),
        previous
    );
    let mut revision_only = historical;
    let revision = revision_only["pin_plan"]["rule_revision"].clone();
    revision_only.as_object_mut().unwrap().remove("pin_plan");
    revision_only["rule_revision"] = revision;
    assert!(
        decode_legacy_v2_intent(&revision_only.to_string())
            .unwrap_err()
            .to_string()
            .contains("needs_attention")
    );
}

#[test]
fn r25_exact_old_alias_and_index_share_the_existing_allocation_identity() {
    let mut config = fixture();
    config.providers[0].identity.provider_id = "noop-prod".into();
    config.providers[0].identity.scope = "account:prod".into();
    config.providers[0].identity.storage_domain = "noop:prod".into();
    let normalized =
        crate::pinning::coordinator::normalize_validated_config(config.clone()).unwrap();
    let mut zip = enabled();
    zip.pin_output_rules = vec![rule(
        "exact-old",
        "exports/",
        ZipRuleEffect::Allow,
        Some(normalized.policies[0].identity.clone()),
    )];
    validate_zip_config(&zip, &config).unwrap();
    let captured = capture(&config, &zip, false, &[], &[]).unwrap();
    let source = bound("exports/archive.zip", "source");
    let outputs = [bound("exports/a", "output")];
    let selected =
        select_zip_intents(&captured, &manifest(&outputs), Some(&source), &outputs).unwrap();
    assert_eq!(
        selected.leases[0].intent.policy_id,
        normalized.policies[0].identity
    );
    assert_eq!(
        selected.leases[0].intent.providers,
        normalized.policies[0].providers
    );
    assert_eq!(
        selected.leases[0].providers[0].config_name,
        normalized.providers[0].name
    );
    zip.pin_output_rules[0].policy_id = Some(config.policies[0].identity.clone());
    assert!(validate_zip_config(&zip, &config).is_err()); // no fuzzy pre-normalization alias
}
