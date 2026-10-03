//! Pure ZIP v2 remote-pin planning. Call only after the final published-version
//! set is known; this module does not grant an archive's manual intent to entries.

use std::collections::BTreeSet;

use anyhow::ensure;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    config::{PolicyTrigger, ProviderMode, ValidatedPinningConfig, ValidatedPolicy},
    identity::ProviderRouteSnapshot,
    policy::{LeaseIntent, LeaseSource},
    tags::ContentMode,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ZipRuleEffect {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZipOutputRuleConfig {
    pub name: String,
    pub priority: u32,
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    pub effect: ZipRuleEffect,
    /// Exact `ValidatedPolicy.identity` (including its list index); changing
    /// legacy policy order invalidates this reference. Required for allow,
    /// forbidden for deny. Never an endpoint or credential supplied by a client.
    pub policy_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZipTargets {
    pub source: bool,
    pub extracted: bool,
}

/// Input must be a successfully *published* exact object version, not a pending
/// extraction or a lookup of the current key. The publisher must supply the
/// final winner list (including its own duplicate-key semantics).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZipPublishedOutput {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub cid: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ZipPlanWarning {
    NoMatchingRule,
    Denied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ZipOutputKind {
    Source,
    Extracted,
}

/// A route reference, not credentials or a client-provided URL. Publication
/// must compare this captured revision with the current provider before IO.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZipSelectedProvider {
    pub config_name: String,
    pub route: ProviderRouteSnapshot,
    /// Availability at capture; a one-mode fallback keeps disabled candidates
    /// for the normal evaluator/worker ordering but must never execute them.
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZipPlannedIntent {
    pub intent: LeaseIntent,
    pub providers: Vec<ZipSelectedProvider>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZipOutputDecision {
    pub kind: ZipOutputKind,
    pub output: ZipPublishedOutput,
    pub rule_name: Option<String>,
    pub warning: Option<ZipPlanWarning>,
    pub intents: Vec<ZipPlannedIntent>,
}

/// Pure decision snapshot, NOT executable work. A publisher must attach it
/// atomically to these exact versions, check revision/identity/quota and use
/// its normal fenced allocation path; this type never submits remote pins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZipOutputPinPlan {
    pub rule_revision: String,
    pub targets: ZipTargets,
    pub outputs: Vec<ZipOutputDecision>,
}

#[derive(Debug, Clone)]
struct Rule {
    raw: ZipOutputRuleConfig,
    policy: Option<ValidatedPolicy>,
    providers: Vec<ZipSelectedProvider>,
}

#[derive(Debug, Clone)]
pub struct ValidatedZipOutputRules {
    rules: Vec<Rule>,
    revision: String,
}

impl ValidatedZipOutputRules {
    /// Explicit v2 opt-in. Never changes the existing evaluator's ordered
    /// first-match behavior. An empty list is a valid deny-by-default v2 set.
    pub fn compile(
        raw: &[ZipOutputRuleConfig],
        pinning: &ValidatedPinningConfig,
    ) -> anyhow::Result<Self> {
        let mut names = BTreeSet::new();
        let mut rules = Vec::with_capacity(raw.len());
        for item in raw {
            ensure!(
                !item.name.is_empty() && names.insert(item.name.as_str()),
                "ZIP output rule has an empty or duplicate name"
            );
            ensure!(
                !item.bucket.is_empty() && (!item.bucket.contains('*') || item.bucket == "*"),
                "ZIP output rule bucket must be exact or `*`"
            );
            let policy = match item.effect {
                ZipRuleEffect::Deny => {
                    ensure!(
                        item.policy_id.is_none(),
                        "ZIP deny rule cannot reference a policy"
                    );
                    None
                }
                ZipRuleEffect::Allow => {
                    let id = item
                        .policy_id
                        .as_deref()
                        .filter(|id| !id.is_empty())
                        .ok_or_else(|| anyhow::anyhow!("ZIP allow rule needs a policy identity"))?;
                    let policy = pinning
                        .policies
                        .iter()
                        .find(|policy| policy.identity == id)
                        .ok_or_else(|| {
                            anyhow::anyhow!("ZIP rule references unknown policy identity")
                        })?;
                    // Never widen a referenced policy's bucket/prefix with an output rule.
                    ensure!(
                        (policy.bucket == "*" || item.bucket == policy.bucket)
                            && item.prefix.starts_with(&policy.prefix),
                        "ZIP output rule exceeds its referenced policy scope"
                    );
                    Some(policy.clone())
                }
            };
            let mut providers = Vec::new();
            if let Some(policy) = &policy {
                for name in &policy.providers {
                    let provider = pinning
                        .providers
                        .iter()
                        .find(|provider| &provider.name == name)
                        .ok_or_else(|| anyhow::anyhow!("ZIP policy provider is unavailable"))?;
                    let enabled = provider.limits.enabled && !provider.identity.retired;
                    // A cross-process capture cannot trust a legacy remote route;
                    // its credentials/endpoint have no durable revision fence.
                    ensure!(
                        !enabled
                            || provider.kind == super::config::ProviderKind::Noop
                            || !provider.identity.provider_id.starts_with("legacy:"),
                        "ZIP remote provider requires explicit identity and revisions"
                    );
                    providers.push(ZipSelectedProvider {
                        config_name: provider.name.clone(),
                        route: provider.identity.route_snapshot(),
                        enabled,
                    });
                }
                if policy.trigger == PolicyTrigger::Always {
                    ensure!(
                        providers.iter().any(|provider| provider.enabled)
                            && (policy.provider_mode != ProviderMode::All
                                || providers.iter().all(|provider| provider.enabled)),
                        "ZIP automatic policy lacks its required enabled providers"
                    );
                }
            }
            rules.push(Rule {
                raw: item.clone(),
                policy,
                providers,
            });
        }
        rules.sort_by(|left, right| {
            left.raw
                .priority
                .cmp(&right.raw.priority)
                .then_with(|| left.raw.name.cmp(&right.raw.name))
        });

        // Canonical, order-independent digest of the exact validated rules and
        // selected provider routes. Neither raw endpoints nor tokens enter it.
        let mut hash = Sha256::new();
        hash.update(b"zip-output-rules/v1");
        hash.update((rules.len() as u64).to_be_bytes());
        for rule in &rules {
            for value in [
                rule.raw.name.as_str(),
                &rule.raw.priority.to_string(),
                &rule.raw.bucket,
                &rule.raw.prefix,
                match rule.raw.effect {
                    ZipRuleEffect::Allow => "allow",
                    ZipRuleEffect::Deny => "deny",
                },
                rule.raw.policy_id.as_deref().unwrap_or(""),
            ] {
                hash.update((value.len() as u64).to_be_bytes());
                hash.update(value.as_bytes());
            }
            hash.update((rule.providers.len() as u64).to_be_bytes());
            for provider in &rule.providers {
                let encoded = serde_json::to_vec(provider)?;
                hash.update((encoded.len() as u64).to_be_bytes());
                hash.update(encoded);
                let limits = &pinning.provider_limits[&provider.config_name];
                for value in [
                    limits.enabled.to_string(),
                    limits.priority.to_string(),
                    limits.max_bytes.to_string(),
                    limits.max_pins.to_string(),
                    pinning
                        .providers
                        .iter()
                        .find(|candidate| candidate.name == provider.config_name)
                        .and_then(|candidate| candidate.requests_per_second)
                        .map_or(String::new(), |rate| rate.to_string()),
                ] {
                    hash.update((value.len() as u64).to_be_bytes());
                    hash.update(value.as_bytes());
                }
            }
        }
        Ok(Self {
            rules,
            revision: hex::encode(hash.finalize()),
        })
    }

    pub fn revision(&self) -> &str {
        &self.revision
    }

    /// Intersects signed v2 remote-target limits with independently matched
    /// output rules. This pure step never parses tags, derives a manual lease
    /// from the archive, or waives the caller's existing evaluator, quota,
    /// identity and publication checks (in either strict or warn mode).
    pub fn plan(
        &self,
        targets: ZipTargets,
        source: Option<ZipPublishedOutput>,
        extracted: &[ZipPublishedOutput],
    ) -> anyhow::Result<ZipOutputPinPlan> {
        ensure!(
            !targets.source || source.is_some(),
            "ZIP source target lacks a published source"
        );
        let outputs = source
            .into_iter()
            .filter(|_| targets.source)
            .map(|output| (ZipOutputKind::Source, output))
            .chain(
                extracted
                    .iter()
                    .filter(|_| targets.extracted)
                    .cloned()
                    .map(|output| (ZipOutputKind::Extracted, output)),
            );
        let mut seen = BTreeSet::new();
        let mut decisions = Vec::new();
        for (kind, output) in outputs {
            ensure!(
                !output.bucket.is_empty()
                    && !output.key.is_empty()
                    && !output.version_id.is_empty()
                    && !output.cid.is_empty(),
                "ZIP output lacks a published identity/version/CID"
            );
            ensure!(
                seen.insert((output.bucket.clone(), output.key.clone())),
                "ZIP output list contains duplicate final keys"
            );
            decisions.push(self.decide(kind, output));
        }
        Ok(ZipOutputPinPlan {
            rule_revision: self.revision.clone(),
            targets,
            outputs: decisions,
        })
    }

    fn decide(&self, kind: ZipOutputKind, output: ZipPublishedOutput) -> ZipOutputDecision {
        let matches = |rule: &&Rule| {
            (rule.raw.bucket == "*" || rule.raw.bucket == output.bucket)
                && output.key.starts_with(&rule.raw.prefix)
        };
        // Deny is a safety veto independent of allow priority: a broad always
        // rule must never re-authorize a private child.
        let selected = self
            .rules
            .iter()
            .filter(matches)
            .find(|rule| rule.raw.effect == ZipRuleEffect::Deny)
            .or_else(|| self.rules.iter().find(matches));
        let (rule_name, warning, intents) = match selected {
            None => (None, Some(ZipPlanWarning::NoMatchingRule), Vec::new()),
            Some(rule) if rule.raw.effect == ZipRuleEffect::Deny => (
                Some(rule.raw.name.clone()),
                Some(ZipPlanWarning::Denied),
                Vec::new(),
            ),
            Some(rule) => {
                let policy = rule
                    .policy
                    .as_ref()
                    .expect("validated allow rule has a policy");
                let intents = if policy.trigger == PolicyTrigger::Always {
                    vec![ZipPlannedIntent {
                        intent: LeaseIntent {
                            source: LeaseSource::Automatic,
                            policy_id: policy.identity.clone(),
                            provider_mode: policy.provider_mode,
                            providers: policy.providers.clone(),
                            content_mode: ContentMode::Object,
                            duration: policy.default_duration,
                        },
                        providers: rule.providers.clone(),
                    }]
                } else {
                    Vec::new()
                };
                (Some(rule.raw.name.clone()), None, intents)
            }
        };
        ZipOutputDecision {
            kind,
            output,
            rule_name,
            warning,
            intents,
        }
    }
}

#[cfg(test)]
#[path = "zip_policy/tests.rs"]
mod tests;
