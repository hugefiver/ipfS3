//! ZIP admission capture and pure exact-version selection for both contracts.
//! New publishers consume CapturedZipIntent, never caller-supplied output policy.

use serde::{Deserialize, Serialize};

use super::{identity::ProviderRouteSnapshot, policy::LeaseIntent};

mod capture;
mod historical;
mod rules;
mod select;

pub use capture::{
    CapturedZipControls, CapturedZipIntent, ZipCaptureContext, ZipContract, capture_zip_intent,
};
pub use historical::{decode_legacy_v1_intent, decode_legacy_v2_intent};
pub use rules::{ValidatedZipOutputRules, validate_zip_config};
pub use select::{ZipBoundOutput, ZipIntentSelection, ZipSelectedLease, select_zip_intents};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ZipRuleEffect {
    Allow,
    Deny,
}

/// policy_index is a readable, zero-based config reference. The historical
/// policy_id field remains an exact opaque alias, never a prefix/fuzzy match.
/// Both resolve to the existing policy identity; there is no ZIP policy identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ZipOutputRuleConfig {
    pub name: String,
    pub priority: u32,
    pub bucket: String,
    pub prefix: String,
    pub effect: ZipRuleEffect,
    pub policy_id: Option<String>,
}

impl<'de> Deserialize<'de> for ZipOutputRuleConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            name: String,
            priority: u32,
            bucket: String,
            #[serde(default)]
            prefix: String,
            effect: ZipRuleEffect,
            policy_id: Option<String>,
            policy_index: Option<usize>,
        }
        let raw = Raw::deserialize(deserializer)?;
        if raw.policy_id.is_some() && raw.policy_index.is_some() {
            return Err(serde::de::Error::custom(
                "ZIP rule cannot specify both policy_id and policy_index; migrate to one reference",
            ));
        }
        if raw
            .policy_id
            .as_ref()
            .is_some_and(|id| id.starts_with("@index:"))
        {
            return Err(serde::de::Error::custom(
                "policy_id must be an exact historical identity; use policy_index for an ordered reference",
            ));
        }
        Ok(Self {
            name: raw.name,
            priority: raw.priority,
            bucket: raw.bucket,
            prefix: raw.prefix,
            effect: raw.effect,
            policy_id: raw
                .policy_id
                .or_else(|| raw.policy_index.map(|index| format!("@index:{index}"))),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZipTargets {
    pub source: bool,
    pub extracted: bool,
}

/// Compatibility DTO for old adapters. New core uses ZipBoundOutput with its
/// exact object owner ID and verifies the FinalZipManifest in the transaction.
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
    NoAvailableProvider,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ZipOutputKind {
    Source,
    Extracted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZipSelectedProvider {
    pub config_name: String,
    /// Includes the full Stage 6 confirmation contract, not just identity.
    pub route: ProviderRouteSnapshot,
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

/// Temporary pre-core compatibility result. Remove executable use of this DTO
/// and ValidatedZipOutputRules::plan from adapters/publication during R25-B/C.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZipOutputPinPlan {
    pub rule_revision: String,
    pub targets: ZipTargets,
    pub outputs: Vec<ZipOutputDecision>,
}

#[cfg(test)]
#[path = "zip_policy/tests.rs"]
mod tests;
