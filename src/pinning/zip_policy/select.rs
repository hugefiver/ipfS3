use std::collections::{BTreeMap, BTreeSet};

use anyhow::ensure;
use serde::{Deserialize, Serialize};

use super::{capture::CaptureSemantics, *};
use crate::{
    pinning::{
        config::PolicyTrigger,
        decision::{DecisionEffect, ExtensionDecision},
        policy::LeaseSource,
        tags::{ContentMode, PinControl},
    },
    zip::batch::FinalZipManifest,
};

/// Exact persisted version binding supplied by the locked publication core.
/// object_id is the lease owner FK, not a batch ID or the current key lookup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZipBoundOutput {
    pub object_id: String,
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub cid: String,
    pub logical_size: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZipSelectedLease {
    pub owner: ZipBoundOutput,
    pub intent: LeaseIntent,
    pub providers: Vec<ZipSelectedProvider>,
    pub targets: Vec<ZipBoundOutput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZipIntentSelection {
    pub leases: Vec<ZipSelectedLease>,
    /// Exact real source's decision; absent for source=false. Manual capture is
    /// preserved, with an explicitly selected source intent appended, if any.
    pub source_decision: Option<ExtensionDecision>,
    /// Persist with each exact successful object, including no-intent results.
    pub output_decisions: Vec<(ZipBoundOutput, ExtensionDecision)>,
    pub warnings: Vec<(ZipBoundOutput, ZipPlanWarning)>,
}

fn valid(output: &ZipBoundOutput) -> bool {
    [
        &output.object_id,
        &output.bucket,
        &output.key,
        &output.version_id,
        &output.cid,
    ]
    .iter()
    .all(|value| !value.is_empty())
        && output.logical_size >= 0
}

pub fn select_zip_intents(
    capture: &CapturedZipIntent,
    manifest: &FinalZipManifest,
    source: Option<&ZipBoundOutput>,
    successful: &[ZipBoundOutput],
) -> anyhow::Result<ZipIntentSelection> {
    capture.validate()?;
    ensure!(
        source.is_some() == capture.controls.publish_source,
        "ZIP source binding contradicts captured publication"
    );
    if let Some(source) = source {
        ensure!(
            valid(source) && source.bucket == capture.bucket && source.key == capture.source_key,
            "ZIP source lacks its captured exact version binding"
        );
    }
    ensure!(
        capture.controls.publish_extracted || successful.is_empty(),
        "ZIP capture excludes extracted publication"
    );
    let files = manifest
        .successful
        .iter()
        .map(|file| (&file.object_key, (&file.cid, file.size)))
        .collect::<BTreeMap<_, _>>();
    ensure!(
        files.len() == manifest.successful.len() && files.len() == successful.len(),
        "ZIP bindings must be exactly the final successful winner set"
    );
    let mut keys = BTreeSet::new();
    let mut ids = BTreeSet::new();
    for output in successful {
        ensure!(
            valid(output)
                && output.bucket == capture.bucket
                && output.key.starts_with(&capture.target_prefix)
                && output.key != capture.source_key
                && !output.key.ends_with('/')
                && keys.insert(&output.key)
                && ids.insert(&output.object_id)
                && source.is_none_or(|source| source.object_id != output.object_id)
                && files
                    .get(&output.key)
                    .is_some_and(|(cid, size)| **cid == output.cid && *size == output.logical_size),
            "ZIP binding is not an exact successful published output"
        );
    }
    if let Some(plan) = &capture.historical_plan {
        let expected = usize::from(capture.controls.targets.source)
            + if capture.controls.targets.extracted {
                successful.len()
            } else {
                0
            };
        ensure!(
            plan.outputs.len() == expected,
            "historical ZIP capture needs_attention: final set differs from original frozen plan"
        );
    }
    let mut result = ZipIntentSelection {
        leases: Vec::new(),
        source_decision: source.map(|_| capture.source_decision.clone()),
        output_decisions: Vec::new(),
        warnings: Vec::new(),
    };
    for intent in &capture.source_decision.effective_intents {
        let owner = source.expect("capture validation requires real source for source intents");
        let targets = if intent.content_mode == ContentMode::Decompressed {
            successful.to_vec()
        } else {
            vec![owner.clone()]
        };
        if !targets.is_empty() {
            result.leases.push(ZipSelectedLease {
                owner: owner.clone(),
                intent: intent.clone(),
                targets,
                providers: intent
                    .providers
                    .iter()
                    .map(|name| {
                        capture
                            .source_providers
                            .iter()
                            .find(|p| &p.selected.config_name == name)
                            .expect("capture route evidence validated")
                            .selected
                            .clone()
                    })
                    .collect(),
            });
        }
    }
    for (kind, output) in source
        .into_iter()
        .filter(|_| capture.controls.targets.source)
        .map(|o| (ZipOutputKind::Source, o))
        .chain(successful.iter().map(|o| (ZipOutputKind::Extracted, o)))
    {
        let eligible = match kind {
            ZipOutputKind::Source => capture.controls.targets.source,
            ZipOutputKind::Extracted => capture.controls.targets.extracted,
        };
        let (planned, warning) = if capture.semantics == CaptureSemantics::LegacyV1 || !eligible {
            (Vec::new(), None)
        } else if capture.semantics == CaptureSemantics::LegacyV2 {
            let plan = capture.historical_plan.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "historical ZIP capture needs_attention: missing frozen output decisions"
                )
            })?;
            let old = plan
                .outputs
                .iter()
                .find(|old| {
                    old.kind == kind
                        && old.output.bucket == output.bucket
                        && old.output.key == output.key
                        && old.output.version_id == output.version_id
                        && old.output.cid == output.cid
                })
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "historical ZIP capture needs_attention: missing exact output evidence"
                    )
                })?;
            (old.intents.clone(), old.warning)
        } else {
            let (policy, _, warning) = capture.policy.choose(&output.bucket, &output.key);
            match policy.filter(|p| p.policy.trigger == PolicyTrigger::Always) {
                Some(policy) if policy.available() => {
                    (vec![policy.planned(LeaseSource::Automatic)], warning)
                }
                Some(_) => (Vec::new(), Some(ZipPlanWarning::NoAvailableProvider)),
                None => (Vec::new(), warning),
            }
        };
        if let Some(warning) = warning {
            result.warnings.push((output.clone(), warning));
        }
        if kind == ZipOutputKind::Source {
            let decision = result
                .source_decision
                .as_mut()
                .expect("exact source binding validated");
            decision
                .effective_intents
                .extend(planned.iter().map(|planned| planned.intent.clone()));
            decision.validate_snapshot().map_err(anyhow::Error::msg)?;
        }
        for planned in &planned {
            result.leases.push(ZipSelectedLease {
                owner: output.clone(),
                intent: planned.intent.clone(),
                providers: planned.providers.clone(),
                targets: vec![output.clone()],
            });
        }
        if kind == ZipOutputKind::Extracted {
            let decision = ExtensionDecision {
                effect: DecisionEffect::NoIntent,
                warning: None,
                legacy_unknown: false,
                origin: capture.origin().clone(),
                control_revision: capture.source_decision.control_revision.clone(),
                config_revision: capture.policy_revision().into(),
                control: PinControl::Absent,
                effective_intents: planned.into_iter().map(|p| p.intent).collect(),
            };
            decision.validate_snapshot().map_err(anyhow::Error::msg)?;
            result.output_decisions.push((output.clone(), decision));
        }
    }
    Ok(result)
}

pub(super) fn compatibility_plan(
    rules: &ValidatedZipOutputRules,
    targets: ZipTargets,
    source: Option<ZipPublishedOutput>,
    extracted: &[ZipPublishedOutput],
) -> anyhow::Result<ZipOutputPinPlan> {
    ensure!(
        !targets.source || source.is_some(),
        "ZIP source target lacks a published source"
    );
    let mut seen = BTreeSet::new();
    let mut outputs = Vec::new();
    for (kind, output) in source
        .into_iter()
        .filter(|_| targets.source)
        .map(|o| (ZipOutputKind::Source, o))
        .chain(
            extracted
                .iter()
                .filter(|_| targets.extracted)
                .cloned()
                .map(|o| (ZipOutputKind::Extracted, o)),
        )
    {
        ensure!(
            [&output.bucket, &output.key, &output.version_id, &output.cid]
                .iter()
                .all(|v| !v.is_empty()),
            "ZIP output lacks a published identity/version/CID"
        );
        ensure!(
            seen.insert((output.bucket.clone(), output.key.clone())),
            "ZIP output list contains duplicate final keys"
        );
        let (policy, rule_name, warning) = rules.choose(&output.bucket, &output.key);
        let intents = policy
            .filter(|p| p.policy.trigger == PolicyTrigger::Always && p.available())
            .map(|p| vec![p.planned(LeaseSource::Automatic)])
            .unwrap_or_default();
        outputs.push(ZipOutputDecision {
            kind,
            output,
            rule_name,
            warning,
            intents,
        });
    }
    Ok(ZipOutputPinPlan {
        rule_revision: rules.revision().into(),
        targets,
        outputs,
    })
}
