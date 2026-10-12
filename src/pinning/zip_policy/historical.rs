//! Pre-R25 captures are decoded using only their original frozen evidence.
//! Missing routes/policies are needs_attention, never current-config defaults.
use anyhow::{Context, ensure};
use serde::Deserialize;

use super::{capture::CaptureSemantics, rules::CapturedProvider, *};
use crate::pinning::decision::ExtensionDecision;
use crate::pinning::tags::ZipRootCapture;
use crate::zip::options::{ZipTargets as WireTargets, ZipV2Options};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyV1 {
    bucket: String,
    source_key: String,
    target_prefix: String,
    root_capture: ZipRootCapture,
    result_enabled: bool,
    decision: ExtensionDecision,
    providers: Vec<CapturedProvider>,
}

pub fn decode_legacy_v1_intent(json: &str) -> anyhow::Result<CapturedZipIntent> {
    let old: LegacyV1 = serde_json::from_str(json).context(
        "historical v1 ZIP capture needs_attention: missing original decision or route evidence",
    )?;
    // v1's known original contract always published a real archive and its
    // successful extracted outputs. Its old automatic archive work lives only
    // in the frozen decision, never in new output defaults.
    let controls = CapturedZipControls {
        publish_source: true,
        publish_extracted: true,
        targets: ZipTargets {
            source: false,
            extracted: false,
        },
        targets_explicit: false,
        root: old.root_capture,
        result_enabled: old.result_enabled,
        admission_token: old.decision.control_revision.clone(),
        token_supplied: false,
        auto_pin_outputs: false,
    };
    let empty = crate::pinning::config::ValidatedPinningConfig::from_raw(
        &crate::config::PinningConfig::default(),
        |_| None,
    )?;
    let capture = CapturedZipIntent {
        schema_version: 1,
        contract: ZipContract::V1,
        semantics: CaptureSemantics::LegacyV1,
        bucket: old.bucket,
        source_key: old.source_key,
        target_prefix: old.target_prefix,
        controls,
        source_decision: old.decision,
        source_providers: old.providers,
        policy: ValidatedZipOutputRules::compile(&[], &empty)?,
        historical_plan: None,
    };
    capture.validate()?;
    Ok(capture)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyV2 {
    bucket: String,
    source_key: String,
    target_prefix: String,
    options: ZipV2Options,
    source_decision: ExtensionDecision,
    providers: Vec<CapturedProvider>,
    pin_plan: ZipOutputPinPlan,
}

/// Old revision-only v2 captures do not prove old policy. The old stored final
/// plan, when present, can be adapted; otherwise fail closed for manual recovery.
pub fn decode_legacy_v2_intent(json: &str) -> anyhow::Result<CapturedZipIntent> {
    let old: LegacyV2 = serde_json::from_str(json).context(
        "historical v2 ZIP capture needs_attention: missing original frozen output/route evidence",
    )?;
    let targets = ZipTargets {
        source: matches!(old.options.targets, WireTargets::Source | WireTargets::Both),
        extracted: matches!(
            old.options.targets,
            WireTargets::Extracted | WireTargets::Both
        ),
    };
    ensure!(
        old.options.result_version == 2 && old.pin_plan.targets == targets,
        "historical v2 ZIP capture needs_attention: contradictory target evidence"
    );
    let controls = CapturedZipControls {
        publish_source: old.options.publish_source,
        publish_extracted: old.options.publish_extracted,
        targets,
        targets_explicit: true,
        root: old.options.root_override.map_or(
            ZipRootCapture::Configured(old.options.root_enabled),
            ZipRootCapture::Tagged,
        ),
        result_enabled: true,
        admission_token: old.options.token,
        token_supplied: true,
        auto_pin_outputs: false,
    };
    let empty = crate::pinning::config::ValidatedPinningConfig::from_raw(
        &crate::config::PinningConfig::default(),
        |_| None,
    )?;
    let capture = CapturedZipIntent {
        schema_version: 1,
        contract: ZipContract::V2,
        semantics: CaptureSemantics::LegacyV2,
        bucket: old.bucket,
        source_key: old.source_key,
        target_prefix: old.target_prefix,
        controls,
        source_decision: old.source_decision,
        source_providers: old.providers,
        policy: ValidatedZipOutputRules::compile(&[], &empty)?,
        historical_plan: Some(old.pin_plan),
    };
    capture.validate()?;
    Ok(capture)
}
