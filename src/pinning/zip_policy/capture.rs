use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize};

use super::{rules::CapturedProvider, *};
use crate::{
    config::{DecompressZipConfig, OptionalPinControlMode},
    pinning::{
        config::{PolicyTrigger, ValidatedPinningConfig},
        decision::{DecisionOrigin, ExtensionDecision},
        policy::{LeaseSource, PinPolicyEvaluator, PublicationContext},
        tags::{ContentMode, ObjectTag, PinControl, ZipRootCapture},
    },
    zip::options::{ParsedZipControls, ZipOptions, ZipTargets as WireTargets},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ZipContract {
    V1,
    V2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapturedZipControls {
    pub publish_source: bool,
    pub publish_extracted: bool,
    pub targets: ZipTargets,
    pub targets_explicit: bool,
    pub root: ZipRootCapture,
    pub result_enabled: bool,
    pub admission_token: String,
    pub token_supplied: bool,
    pub auto_pin_outputs: bool,
}

pub struct ZipCaptureContext<'a> {
    pub bucket: &'a str,
    pub source_key: &'a str,
    pub target_prefix: &'a str,
    pub tags: &'a [ObjectTag],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum CaptureSemantics {
    Current,
    LegacyV1,
    LegacyV2,
}

/// Trusted admission snapshot, not an HTTP DTO or a proof minted by a caller.
/// Private fields prevent passing pre-authorized per-output specs to new core.
/// Read persisted JSON via decode_json, verify admission origin in the store
/// guard, then bind the final manifest to actual versions before selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapturedZipIntent {
    pub(super) schema_version: u8,
    pub(super) contract: ZipContract,
    pub(super) semantics: CaptureSemantics,
    pub(super) bucket: String,
    pub(super) source_key: String,
    pub(super) target_prefix: String,
    pub(super) controls: CapturedZipControls,
    pub(super) source_decision: ExtensionDecision,
    pub(super) source_providers: Vec<CapturedProvider>,
    pub(super) policy: ValidatedZipOutputRules,
    pub(super) historical_plan: Option<ZipOutputPinPlan>,
}

impl CapturedZipIntent {
    pub fn schema_version(&self) -> u8 {
        self.schema_version
    }
    pub fn contract(&self) -> ZipContract {
        self.contract
    }
    pub fn origin(&self) -> &DecisionOrigin {
        &self.source_decision.origin
    }
    pub fn controls(&self) -> &CapturedZipControls {
        &self.controls
    }
    pub fn source_decision(&self) -> &ExtensionDecision {
        &self.source_decision
    }
    pub fn bucket(&self) -> &str {
        &self.bucket
    }
    pub fn source_key(&self) -> &str {
        &self.source_key
    }
    pub fn target_prefix(&self) -> &str {
        &self.target_prefix
    }
    pub fn policy_revision(&self) -> &str {
        self.policy.revision()
    }

    /// Unknown input uses the bounded original scope; a guard-verified manifest
    /// uses only actual duties. This read never binds an owner or grants a lease.
    pub(crate) fn io_provider_candidates(
        &self,
        manifest: Option<&crate::zip::batch::FinalZipManifest>,
    ) -> Vec<&ZipSelectedProvider> {
        let mut providers = self
            .source_providers
            .iter()
            .filter(|provider| {
                self.source_decision.effective_intents.iter().any(|intent| {
                    let has_subject = match intent.content_mode {
                        ContentMode::Object => self.controls.publish_source,
                        ContentMode::Decompressed => {
                            self.controls.publish_extracted
                                && manifest.is_none_or(|manifest| !manifest.successful.is_empty())
                        }
                    };
                    has_subject && intent.providers.contains(&provider.selected.config_name)
                })
            })
            .map(|provider| &provider.selected)
            .collect::<Vec<_>>();
        match self.semantics {
            CaptureSemantics::Current => {
                if self.controls.publish_source
                    && self.controls.targets.source
                    && let Some(policy) = self
                        .policy
                        .choose(&self.bucket, &self.source_key)
                        .0
                        .filter(|policy| {
                            policy.policy.trigger == PolicyTrigger::Always && policy.available()
                        })
                {
                    providers.extend(policy.providers.iter().map(|provider| &provider.selected));
                }
                if self.controls.publish_extracted && self.controls.targets.extracted {
                    if let Some(manifest) = manifest {
                        for file in &manifest.successful {
                            if let Some(policy) =
                                self.policy.choose(&self.bucket, &file.object_key).0
                                && policy.policy.trigger == PolicyTrigger::Always
                                && policy.available()
                            {
                                providers.extend(
                                    policy.providers.iter().map(|provider| &provider.selected),
                                );
                            }
                        }
                    } else {
                        for policy in self
                            .policy
                            .output_candidates(&self.bucket, &self.target_prefix)
                        {
                            providers
                                .extend(policy.providers.iter().map(|provider| &provider.selected));
                        }
                    }
                }
            }
            CaptureSemantics::LegacyV1 => {}
            CaptureSemantics::LegacyV2 => {
                if let Some(plan) = &self.historical_plan {
                    providers.extend(
                        plan.outputs
                            .iter()
                            .filter(|output| {
                                output.output.bucket == self.bucket
                                    && match output.kind {
                                        ZipOutputKind::Source => {
                                            self.controls.publish_source
                                                && self.controls.targets.source
                                                && output.output.key == self.source_key
                                        }
                                        ZipOutputKind::Extracted => {
                                            self.controls.publish_extracted
                                                && self.controls.targets.extracted
                                                && manifest.map_or_else(
                                                    || {
                                                        output
                                                            .output
                                                            .key
                                                            .starts_with(&self.target_prefix)
                                                    },
                                                    |manifest| {
                                                        manifest.successful.iter().any(|file| {
                                                            file.object_key == output.output.key
                                                        })
                                                    },
                                                )
                                        }
                                    }
                            })
                            .flat_map(|output| &output.intents)
                            .flat_map(|intent| &intent.providers),
                    );
                }
            }
        }
        providers
    }

    /// Read-only qualification for a safety veto, not a publication binding or
    /// lease selection. Use the same frozen choose/availability primitives.
    pub(crate) fn automatic_output_eligible(&self, kind: ZipOutputKind, key: &str) -> bool {
        let targeted = match kind {
            ZipOutputKind::Source => self.controls.targets.source,
            ZipOutputKind::Extracted => self.controls.targets.extracted,
        };
        if !targeted {
            return false;
        }
        match self.semantics {
            CaptureSemantics::Current => {
                self.policy
                    .choose(&self.bucket, key)
                    .0
                    .is_some_and(|policy| {
                        policy.policy.trigger == PolicyTrigger::Always && policy.available()
                    })
            }
            CaptureSemantics::LegacyV1 => false,
            CaptureSemantics::LegacyV2 => self.historical_plan.as_ref().is_some_and(|plan| {
                plan.outputs.iter().any(|old| {
                    old.kind == kind
                        && old.output.bucket == self.bucket
                        && old.output.key == key
                        && !old.intents.is_empty()
                })
            }),
        }
    }

    pub(crate) fn automatic_extracted_scope_eligible(&self, prefix: &str) -> bool {
        if !self.controls.targets.extracted {
            return false;
        }
        match self.semantics {
            CaptureSemantics::Current => !self
                .policy
                .output_candidates(&self.bucket, prefix)
                .is_empty(),
            CaptureSemantics::LegacyV1 => false,
            CaptureSemantics::LegacyV2 => self.historical_plan.as_ref().is_some_and(|plan| {
                plan.outputs.iter().any(|old| {
                    old.kind == ZipOutputKind::Extracted
                        && old.output.bucket == self.bucket
                        && old.output.key.starts_with(prefix)
                        && !old.intents.is_empty()
                })
            }),
        }
    }

    pub(crate) fn originally_denied_scope(&self, prefix: &str) -> bool {
        self.policy.rules.as_ref().is_some_and(|rules| {
            rules.iter().any(|rule| {
                rule.raw.effect == ZipRuleEffect::Deny
                    && (rule.raw.bucket == "*" || rule.raw.bucket == self.bucket)
                    && prefix.starts_with(&rule.raw.prefix)
            })
        })
    }

    /// Frozen effective options for fingerprint/persist/replay adapters. No
    /// adapter needs to derive targets or consult current defaults a second time.
    pub fn v2_options(&self) -> Option<crate::zip::options::ZipV2Options> {
        (self.contract == ZipContract::V2).then(|| crate::zip::options::ZipV2Options {
            publish_source: self.controls.publish_source,
            publish_extracted: self.controls.publish_extracted,
            targets: match (
                self.controls.targets.source,
                self.controls.targets.extracted,
            ) {
                (false, false) => WireTargets::None,
                (true, false) => WireTargets::Source,
                (false, true) => WireTargets::Extracted,
                (true, true) => WireTargets::Both,
            },
            token: self.controls.admission_token.clone(),
            root_override: match self.controls.root {
                ZipRootCapture::Tagged(value) => Some(value),
                _ => None,
            },
            root_enabled: self.controls.root.enabled(),
            result_version: 2,
        })
    }

    pub fn provider_limits(
        &self,
        provider_id: &str,
    ) -> Option<&crate::pinning::config::ProviderLimits> {
        self.source_providers
            .iter()
            .chain(
                self.policy
                    .policies
                    .iter()
                    .flat_map(|policy| &policy.providers),
            )
            .find(|provider| provider.selected.route.provider_id == provider_id)
            .map(|provider| &provider.limits)
    }

    pub fn decode_json(json: &str) -> anyhow::Result<Self> {
        let capture: Self = serde_json::from_str(json)
            .context("ZIP capture needs_attention: missing or unknown capture version/evidence")?;
        capture.validate()?;
        Ok(capture)
    }

    pub(super) fn validate(&self) -> anyhow::Result<()> {
        let route_complete = |route: &crate::pinning::identity::ProviderRouteSnapshot| {
            route.resource_type() != crate::pinning::identity::RemoteResourceType::ClusterPin
                || route.confirmation.is_some()
        };
        ensure!(
            self.schema_version == 1,
            "ZIP capture needs_attention: unsupported schema version"
        );
        self.source_decision
            .validate_snapshot()
            .map_err(anyhow::Error::msg)?;
        ensure!(
            !self.bucket.is_empty()
                && !self.source_key.is_empty()
                && crate::zip::sanitize::normalize_target_prefix(&self.target_prefix)?
                    == self.target_prefix,
            "ZIP capture needs_attention: invalid destination"
        );
        ensure!(
            self.controls.publish_source || self.controls.publish_extracted,
            "ZIP capture needs_attention: no published output kind"
        );
        ensure!(
            !self.controls.targets.source || self.controls.publish_source,
            "ZIP capture needs_attention: source target lacks a published source"
        );
        ensure!(
            !self.controls.targets.extracted || self.controls.publish_extracted,
            "ZIP capture needs_attention: extracted targets lack published outputs"
        );
        ensure!(
            self.controls.publish_source || self.source_decision.effective_intents.is_empty(),
            "ZIP capture needs_attention: manual owner lacks a published source"
        );
        ensure!(
            !self.controls.admission_token.is_empty()
                && self.controls.admission_token.len() <= 128
                && self
                    .controls
                    .admission_token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._~-".contains(&b)),
            "ZIP capture needs_attention: invalid admission token"
        );
        ensure!(
            self.contract != ZipContract::V2
                || (self.controls.result_enabled
                    && (self.controls.publish_source || self.controls.token_supplied)),
            "ZIP capture needs_attention: invalid v2 negotiation"
        );
        ensure!(
            self.semantics != CaptureSemantics::Current
                || (self.historical_plan.is_none()
                    && self
                        .source_decision
                        .effective_intents
                        .iter()
                        .all(|intent| intent.source == LeaseSource::Manual)
                    && (self.controls.publish_source
                        || self.source_decision.control == PinControl::Absent)),
            "ZIP capture needs_attention: current source decision contradicts captured source/manual contract"
        );
        for intent in &self.source_decision.effective_intents {
            ensure!(
                intent.content_mode != ContentMode::Decompressed || self.controls.publish_extracted,
                "ZIP capture needs_attention: decompressed intent lacks outputs"
            );
            ensure!(
                intent.providers.iter().all(|name| self
                    .source_providers
                    .iter()
                    .any(|p| &p.selected.config_name == name)),
                "ZIP capture needs_attention: missing original provider route evidence"
            );
        }
        ensure!(
            self.source_providers
                .iter()
                .chain(
                    self.policy
                        .policies
                        .iter()
                        .flat_map(|policy| &policy.providers)
                )
                .all(|provider| route_complete(&provider.selected.route)),
            "ZIP capture needs_attention: missing original Stage 6 confirmation evidence"
        );
        if let Some(plan) = &self.historical_plan {
            ensure!(
                self.semantics == CaptureSemantics::LegacyV2
                    && plan.targets == self.controls.targets,
                "historical ZIP capture needs_attention: contradictory frozen plan"
            );
            let mut seen = std::collections::BTreeSet::new();
            for output in &plan.outputs {
                ensure!(
                    output.output.bucket == self.bucket
                        && !output.output.key.is_empty()
                        && !output.output.version_id.is_empty()
                        && !output.output.cid.is_empty()
                        && seen.insert((&output.output.bucket, &output.output.key))
                        && match output.kind {
                            ZipOutputKind::Source =>
                                self.controls.targets.source && output.output.key == self.source_key,
                            ZipOutputKind::Extracted =>
                                self.controls.targets.extracted
                                    && output.output.key.starts_with(&self.target_prefix)
                                    && output.output.key != self.source_key,
                        },
                    "historical ZIP capture needs_attention: invalid frozen output bindings"
                );
                for intent in &output.intents {
                    ensure!(
                        intent.intent.source == LeaseSource::Automatic
                            && intent.intent.content_mode == ContentMode::Object
                            && intent.intent.providers.len() == intent.providers.len()
                            && intent
                                .intent
                                .providers
                                .iter()
                                .zip(&intent.providers)
                                .all(|(name, provider)| name == &provider.config_name),
                        "historical ZIP capture needs_attention: incomplete original provider evidence"
                    );
                    ensure!(
                        intent
                            .providers
                            .iter()
                            .all(|provider| route_complete(&provider.route)),
                        "historical ZIP capture needs_attention: missing original Stage 6 confirmation evidence"
                    );
                }
            }
        }
        Ok(())
    }
}

pub fn capture_zip_intent(
    config: &ValidatedPinningConfig,
    zip: &DecompressZipConfig,
    mode: OptionalPinControlMode,
    context: ZipCaptureContext<'_>,
    parsed: &ParsedZipControls,
    origin: DecisionOrigin,
) -> anyhow::Result<CapturedZipIntent> {
    let normalized = super::rules::normalized_for_zip(config)?;
    let config = normalized.as_ref();
    ensure!(
        PinControl::from_tags(context.tags)? == parsed.pin,
        "ZIP capture controls do not match authenticated tags"
    );
    ensure!(
        parsed
            .target_prefix
            .as_deref()
            .is_none_or(|prefix| prefix == context.target_prefix),
        "ZIP capture prefix does not match authenticated controls"
    );
    let policy = ValidatedZipOutputRules::from_config(zip, config)?;
    let auto_pin_outputs = zip.auto_pin_outputs.unwrap_or(false);
    let output_candidates = policy.output_candidates(context.bucket, context.target_prefix);
    let permits_outputs = !output_candidates.is_empty();
    let (contract, controls) = match &parsed.options {
        ZipOptions::Legacy => (
            ZipContract::V1,
            CapturedZipControls {
                publish_source: true,
                publish_extracted: true,
                targets: ZipTargets {
                    source: false,
                    extracted: auto_pin_outputs && permits_outputs,
                },
                targets_explicit: false,
                root: parsed.root,
                result_enabled: parsed.result_enabled,
                admission_token: uuid::Uuid::new_v4().to_string(),
                token_supplied: false,
                auto_pin_outputs,
            },
        ),
        ZipOptions::V2(v2) => (
            ZipContract::V2,
            CapturedZipControls {
                publish_source: v2.publish_source,
                publish_extracted: v2.publish_extracted,
                targets: if parsed.targets_explicit {
                    ZipTargets {
                        source: matches!(v2.targets, WireTargets::Source | WireTargets::Both),
                        extracted: matches!(v2.targets, WireTargets::Extracted | WireTargets::Both),
                    }
                } else {
                    ZipTargets {
                        source: false,
                        extracted: v2.publish_extracted && auto_pin_outputs && permits_outputs,
                    }
                },
                targets_explicit: parsed.targets_explicit,
                root: parsed.root,
                result_enabled: parsed.result_enabled,
                admission_token: v2.token.clone(),
                token_supplied: parsed.token_supplied,
                auto_pin_outputs,
            },
        ),
    };
    ensure!(
        controls.publish_source || matches!(parsed.pin, PinControl::Absent),
        "archive-owned manual pinning requires a published source"
    );
    let (mut source_policy, mut source_decision) = PinPolicyEvaluator::with_mode(config, mode)
        .evaluate_publication_decision(
            PublicationContext {
                bucket: context.bucket,
                key: context.source_key,
                tags: context.tags,
                is_decompress_zip: true,
            },
            origin,
        )?;
    // New ZIP archive policy only owns its explicitly authorized manual group.
    // Automatic selection happens later from frozen policy + exact outputs.
    source_policy
        .leases
        .retain(|intent| intent.source == LeaseSource::Manual);
    source_decision.effective_intents = source_policy.leases.clone();
    source_decision
        .validate_policy(&source_policy)
        .map_err(anyhow::Error::msg)?;
    source_decision
        .capture_durable_revision(config, mode)
        .map_err(anyhow::Error::msg)?;
    let names = source_decision
        .effective_intents
        .iter()
        .flat_map(|intent| intent.providers.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let source_providers = super::rules::capture_providers(&names, config)?;
    // Only potentially executable candidates need durable remote evidence.
    if controls.targets.extracted {
        for p in output_candidates {
            p.ensure_durable()?;
        }
    }
    if controls.targets.source
        && let Some(p) = policy
            .choose(context.bucket, context.source_key)
            .0
            .filter(|p| p.policy.trigger == PolicyTrigger::Always && p.available())
    {
        p.ensure_durable()?;
    }
    let capture = CapturedZipIntent {
        schema_version: 1,
        contract,
        semantics: CaptureSemantics::Current,
        bucket: context.bucket.into(),
        source_key: context.source_key.into(),
        target_prefix: context.target_prefix.into(),
        controls,
        source_decision,
        source_providers,
        policy,
        historical_plan: None,
    };
    capture.validate()?;
    Ok(capture)
}
