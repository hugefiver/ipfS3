use std::{borrow::Cow, collections::BTreeSet};

use anyhow::{anyhow, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::*;
use crate::{
    config::DecompressZipConfig,
    pinning::config::{
        PolicyTrigger, ProviderKind, ProviderLimits, ProviderMode, ValidatedPinningConfig,
        ValidatedPolicy,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CapturedProvider {
    pub selected: ZipSelectedProvider,
    pub limits: ProviderLimits,
    pub requests_per_second: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CapturedPolicy {
    pub policy: ValidatedPolicy,
    pub providers: Vec<CapturedProvider>,
}

pub(super) fn capture_providers(
    names: &[String],
    config: &ValidatedPinningConfig,
) -> anyhow::Result<Vec<CapturedProvider>> {
    names
        .iter()
        .map(|name| {
            let provider = config
                .providers
                .iter()
                .find(|provider| &provider.name == name)
                .ok_or_else(|| anyhow!("ZIP captured policy provider is unavailable"))?;
            Ok(CapturedProvider {
                selected: ZipSelectedProvider {
                    config_name: name.clone(),
                    route: provider.route_snapshot(),
                    enabled: provider.limits.enabled && !provider.identity.retired,
                },
                limits: provider.limits.clone(),
                requests_per_second: provider.requests_per_second,
            })
        })
        .collect()
}

impl CapturedPolicy {
    pub fn available(&self) -> bool {
        self.providers
            .iter()
            .any(|provider| provider.selected.enabled)
            && (self.policy.provider_mode != ProviderMode::All
                || self
                    .providers
                    .iter()
                    .all(|provider| provider.selected.enabled))
    }

    pub fn ensure_durable(&self) -> anyhow::Result<()> {
        for provider in &self.providers {
            ensure!(
                !provider.selected.enabled
                    || provider.selected.route.backend == "noop"
                    || !provider.selected.route.provider_id.starts_with("legacy:"),
                "ZIP remote provider requires explicit identity and revisions"
            );
        }
        Ok(())
    }

    pub fn planned(&self, source: crate::pinning::policy::LeaseSource) -> ZipPlannedIntent {
        ZipPlannedIntent {
            intent: crate::pinning::policy::lease_intent(
                &self.policy,
                source,
                crate::pinning::tags::ContentMode::Object,
                self.policy.default_duration,
            ),
            providers: self.providers.iter().map(|p| p.selected.clone()).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Rule {
    pub raw: ZipOutputRuleConfig,
    pub policy: Option<CapturedPolicy>,
}

/// A frozen common restriction set. None means ordinary ordered policies;
/// Some([]) is an explicit deny-by-default set, not an omitted default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidatedZipOutputRules {
    pub(super) rules: Option<Vec<Rule>>,
    pub(super) policies: Vec<CapturedPolicy>,
    revision: String,
}

/// Use the coordinator's established allocation namespace. A config already
/// normalized by startup stays borrowed, with no second identity or alias map.
pub(super) fn normalized_for_zip(
    config: &ValidatedPinningConfig,
) -> anyhow::Result<Cow<'_, ValidatedPinningConfig>> {
    if config
        .providers
        .iter()
        .all(|provider| provider.name == provider.identity.allocation_key())
    {
        Ok(Cow::Borrowed(config))
    } else {
        crate::pinning::coordinator::normalize_validated_config(config.clone()).map(Cow::Owned)
    }
}

pub fn validate_zip_config(
    zip: &DecompressZipConfig,
    config: &ValidatedPinningConfig,
) -> anyhow::Result<()> {
    validate_migration(zip, config)?;
    // No ZIP references means no new early coordinator/ordinary-config gate.
    // Existing alias validation stays at its established startup boundary.
    if let Some(raw) = zip.output_rules() {
        let normalized = normalized_for_zip(config)?;
        ValidatedZipOutputRules::compile_optional(Some(raw), &normalized)?;
    }
    Ok(())
}

fn validate_migration(
    zip: &DecompressZipConfig,
    config: &ValidatedPinningConfig,
) -> anyhow::Result<()> {
    let old_remote_auto = config.policies.iter().enumerate().any(|(index, policy)| {
        policy.trigger == PolicyTrigger::Always
            && !config.policies[..index].iter().any(|prior| {
                (prior.bucket == "*" || prior.bucket == policy.bucket)
                    && policy.prefix.starts_with(&prior.prefix)
            })
            && policy.providers.iter().any(|name| {
                config.providers.iter().any(|provider| {
                    &provider.name == name
                        && provider.limits.enabled
                        && !provider.identity.retired
                        && provider.kind != ProviderKind::Noop
                })
            })
    });
    ensure!(
        zip.auto_pin_outputs.is_some() || (!old_remote_auto && zip.output_rules().is_none()),
        "ZIP configuration migration required: set [decompress_zip].auto_pin_outputs = true for extracted-output automatic pinning, or false to disable ZIP automatic pinning without changing non-ZIP/manual behavior"
    );
    Ok(())
}

impl ValidatedZipOutputRules {
    /// Temporary source compatibility for the pre-core v2 callers. Its slice
    /// always means explicitly supplied rules, including a deny-by-default [].
    pub fn compile(
        raw: &[ZipOutputRuleConfig],
        config: &ValidatedPinningConfig,
    ) -> anyhow::Result<Self> {
        let mut compiled = Self::compile_optional(Some(raw), config)?;
        if raw.is_empty() {
            // The pre-core empty rule set never selects a policy. Preserve its
            // old replay fingerprint; new captures use compile_optional below
            // and still bind the effective policy snapshot.
            let mut hash = Sha256::new();
            hash.update(b"zip-output-rules/v1");
            hash.update(0_u64.to_be_bytes());
            compiled.revision = hex::encode(hash.finalize());
        }
        for policy in compiled
            .rules
            .as_ref()
            .into_iter()
            .flatten()
            .filter_map(|rule| rule.policy.as_ref())
        {
            if policy.policy.trigger == PolicyTrigger::Always {
                policy.ensure_durable()?;
                ensure!(
                    policy.available(),
                    "ZIP automatic policy lacks its required enabled providers"
                );
            }
        }
        Ok(compiled)
    }

    pub fn from_config(
        zip: &DecompressZipConfig,
        config: &ValidatedPinningConfig,
    ) -> anyhow::Result<Self> {
        validate_migration(zip, config)?;
        // Resolve historical IDs in the same namespace old coordinator used,
        // including explicit identities and provider allocation aliases.
        let config = normalized_for_zip(config)?;
        Self::compile_optional(zip.output_rules(), &config)
    }

    fn compile_optional(
        raw: Option<&[ZipOutputRuleConfig]>,
        config: &ValidatedPinningConfig,
    ) -> anyhow::Result<Self> {
        let policies = config
            .policies
            .iter()
            .map(|policy| {
                Ok(CapturedPolicy {
                    policy: policy.clone(),
                    providers: capture_providers(&policy.providers, config)?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let rules = raw.map(|raw| {
            let mut names = BTreeSet::new();
            let mut rules = Vec::with_capacity(raw.len());
            for item in raw {
                ensure!(!item.name.is_empty() && names.insert(&item.name), "ZIP output rule has an empty or duplicate name");
                ensure!(!item.bucket.is_empty() && (!item.bucket.contains('*') || item.bucket == "*"), "ZIP output rule bucket must be exact or `*`");
                let mut raw = item.clone();
                let policy = match item.effect {
                    ZipRuleEffect::Deny => {
                        ensure!(item.policy_id.is_none(), "ZIP deny rule cannot reference a policy");
                        None
                    }
                    ZipRuleEffect::Allow => {
                        let reference = item.policy_id.as_deref().ok_or_else(|| anyhow!("ZIP allow rule needs policy_index or an exact historical policy_id"))?;
                        let policy = if let Some(index) = reference.strip_prefix("@index:") {
                            index.parse::<usize>().ok().and_then(|index| policies.get(index))
                        } else {
                            policies.iter().find(|policy| policy.policy.identity == reference)
                        }.ok_or_else(|| anyhow!("ZIP rule references unknown policy identity or policy_index; migrate to a valid policy_index or exact policy_id"))?;
                        ensure!((policy.policy.bucket == "*" || item.bucket == policy.policy.bucket)
                            && item.prefix.starts_with(&policy.policy.prefix), "ZIP output rule exceeds its referenced policy scope");
                        raw.policy_id = Some(policy.policy.identity.clone());
                        Some(policy.clone())
                    }
                };
                rules.push(Rule { raw, policy });
            }
            rules.sort_by(|a, b| a.raw.priority.cmp(&b.raw.priority).then_with(|| a.raw.name.cmp(&b.raw.name)));
            Ok(rules)
        }).transpose()?;
        let encoded = serde_json::to_vec(&(&rules, &policies))?;
        let mut hash = Sha256::new();
        hash.update(b"zip-captured-policy/v1");
        hash.update(encoded);
        Ok(Self {
            rules,
            policies,
            revision: hex::encode(hash.finalize()),
        })
    }

    pub fn revision(&self) -> &str {
        &self.revision
    }

    /// Admission can conservatively establish that some real output in the
    /// destination scope is authorized. Final per-key selection remains choose.
    pub(super) fn output_candidates(&self, bucket: &str, prefix: &str) -> Vec<&CapturedPolicy> {
        let overlaps = |policy_bucket: &str, policy_prefix: &str| {
            (policy_bucket == "*" || policy_bucket == bucket)
                && (policy_prefix.starts_with(prefix) || prefix.starts_with(policy_prefix))
        };
        let narrower = |policy_prefix: &str| {
            if policy_prefix.len() > prefix.len() {
                policy_prefix.to_owned()
            } else {
                prefix.to_owned()
            }
        };
        if let Some(rules) = &self.rules {
            rules
                .iter()
                .enumerate()
                .filter_map(|(index, rule)| {
                    if rule.raw.effect != ZipRuleEffect::Allow
                        || !overlaps(&rule.raw.bucket, &rule.raw.prefix)
                    {
                        return None;
                    }
                    let scope = narrower(&rule.raw.prefix);
                    if rules.iter().any(|deny| {
                        deny.raw.effect == ZipRuleEffect::Deny
                            && (deny.raw.bucket == "*" || deny.raw.bucket == bucket)
                            && scope.starts_with(&deny.raw.prefix)
                    }) {
                        return None;
                    }
                    if rules[..index].iter().any(|prior| {
                        prior.raw.effect == ZipRuleEffect::Allow
                            && (prior.raw.bucket == "*" || prior.raw.bucket == bucket)
                            && scope.starts_with(&prior.raw.prefix)
                    }) {
                        return None;
                    }
                    rule.policy.as_ref().filter(|policy| {
                        policy.policy.trigger == PolicyTrigger::Always && policy.available()
                    })
                })
                .collect()
        } else {
            self.policies
                .iter()
                .enumerate()
                .filter_map(|(index, policy)| {
                    let scope = narrower(&policy.policy.prefix);
                    (policy.policy.trigger == PolicyTrigger::Always
                        && policy.available()
                        && overlaps(&policy.policy.bucket, &policy.policy.prefix)
                        && !self.policies[..index].iter().any(|prior| {
                            (prior.policy.bucket == "*" || prior.policy.bucket == bucket)
                                && scope.starts_with(&prior.policy.prefix)
                        }))
                    .then_some(policy)
                })
                .collect()
        }
    }

    /// Compatibility only: R25-B/C must delete this current-config adapter
    /// decision flow. New publication accepts only capture + exact bindings.
    pub fn plan(
        &self,
        targets: ZipTargets,
        source: Option<ZipPublishedOutput>,
        extracted: &[ZipPublishedOutput],
    ) -> anyhow::Result<ZipOutputPinPlan> {
        super::select::compatibility_plan(self, targets, source, extracted)
    }

    pub(super) fn choose(
        &self,
        bucket: &str,
        key: &str,
    ) -> (
        Option<&CapturedPolicy>,
        Option<String>,
        Option<ZipPlanWarning>,
    ) {
        if let Some(rules) = &self.rules {
            let matches = |rule: &&Rule| {
                (rule.raw.bucket == "*" || rule.raw.bucket == bucket)
                    && key.starts_with(&rule.raw.prefix)
            };
            match rules
                .iter()
                .filter(matches)
                .find(|rule| rule.raw.effect == ZipRuleEffect::Deny)
                .or_else(|| rules.iter().find(matches))
            {
                None => (None, None, Some(ZipPlanWarning::NoMatchingRule)),
                Some(rule) if rule.raw.effect == ZipRuleEffect::Deny => (
                    None,
                    Some(rule.raw.name.clone()),
                    Some(ZipPlanWarning::Denied),
                ),
                Some(rule) => (rule.policy.as_ref(), Some(rule.raw.name.clone()), None),
            }
        } else {
            let policy = self.policies.iter().find(|p| {
                (p.policy.bucket == "*" || p.policy.bucket == bucket)
                    && key.starts_with(&p.policy.prefix)
            });
            (
                policy,
                None,
                policy.is_none().then_some(ZipPlanWarning::NoMatchingRule),
            )
        }
    }
}
