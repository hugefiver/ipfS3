use anyhow::{bail, ensure};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupMode {
    #[default]
    Retain,
    Managed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Ownership {
    ApplicationCreated,
    ExternalExisting,
    Unknown,
}

impl Ownership {
    pub(crate) fn from_persisted(raw: &str) -> Self {
        match raw {
            "application_created" => Self::ApplicationCreated,
            "external_existing" => Self::ExternalExisting,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinningIdentityConfig {
    #[serde(default)]
    pub primary_storage_domain: Option<String>,
    #[serde(default)]
    pub providers: Vec<ProviderIdentityConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderIdentityConfig {
    pub config_name: String,
    pub provider_id: String,
    pub display_name: String,
    pub backend: String,
    pub scope: String,
    pub storage_domain: String,
    pub credential_revision: u64,
    pub endpoint_revision: u64,
    #[serde(default)]
    pub secret_ref: Option<String>,
    pub api_profile: String,
    pub strategy: String,
    #[serde(default)]
    pub retired: bool,
    #[serde(default)]
    pub cleanup: CleanupMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderIdentity {
    pub provider_id: String,
    pub display_name: String,
    pub backend: String,
    pub scope: String,
    pub storage_domain: String,
    pub credential_revision: u64,
    pub endpoint_revision: u64,
    pub secret_ref: Option<String>,
    pub api_profile: String,
    pub strategy: String,
    pub retired: bool,
    pub cleanup: CleanupMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRouteSnapshot {
    pub provider_id: String,
    pub backend: String,
    pub scope: String,
    pub storage_domain: String,
    pub credential_revision: u64,
    pub endpoint_revision: u64,
    pub secret_ref: Option<String>,
    pub api_profile: String,
    pub strategy: String,
    pub cleanup: CleanupMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteResourceType {
    PsaRequest,
    HostedFile,
    RpcPin,
    ClusterPin,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalResourceKey {
    pub backend: String,
    pub scope: String,
    pub resource_type: RemoteResourceType,
    pub resource_id: String,
}

impl ProviderRouteSnapshot {
    pub fn resource_type(&self) -> RemoteResourceType {
        resource_type(&self.api_profile)
    }
}

fn resource_type(profile: &str) -> RemoteResourceType {
    match profile {
        "pinata-v3" | "pinata-legacy" => RemoteResourceType::HostedFile,
        "rpc" | "kubo" | "filebase-rpc" => RemoteResourceType::RpcPin,
        "cluster" => RemoteResourceType::ClusterPin,
        _ => RemoteResourceType::PsaRequest,
    }
}

impl ProviderIdentity {
    pub fn owns_allocation_domain(&self, key: &str) -> bool {
        key.strip_prefix("domain:")
            .and_then(|raw| serde_json::from_str::<(String, String, RemoteResourceType)>(raw).ok())
            .is_some_and(|(backend, scope, _)| backend == self.backend && scope == self.scope)
    }
    /// Persistent capacity/locking namespace, deliberately independent of aliases,
    /// credentials and transport endpoints. Legacy namespaces are not re-keyed.
    pub fn allocation_key(&self) -> String {
        if let Some(name) = self.provider_id.strip_prefix("legacy:") {
            return name.to_owned();
        }
        format!(
            "domain:{}",
            serde_json::to_string(&(&self.backend, &self.scope, self.resource_type()))
                .expect("string tuple serializes")
        )
    }

    pub fn resource_type(&self) -> RemoteResourceType {
        resource_type(&self.api_profile)
    }

    pub(crate) fn explicit(raw: &ProviderIdentityConfig) -> anyhow::Result<Self> {
        validate_identifier("provider_id", &raw.provider_id)?;
        ensure!(
            !raw.display_name.trim().is_empty(),
            "provider identity display_name must not be empty"
        );
        validate_identifier("backend", &raw.backend)?;
        validate_identifier("scope", &raw.scope)?;
        validate_identifier("storage_domain", &raw.storage_domain)?;
        validate_identifier("api_profile", &raw.api_profile)?;
        validate_identifier("strategy", &raw.strategy)?;
        ensure!(
            raw.credential_revision > 0,
            "provider identity credential_revision must be greater than zero"
        );
        ensure!(
            raw.endpoint_revision > 0,
            "provider identity endpoint_revision must be greater than zero"
        );
        if let Some(secret_ref) = &raw.secret_ref {
            ensure!(
                secret_ref.starts_with("env:")
                    && secret_ref.len() > "env:".len()
                    && !secret_ref["env:".len()..].contains(|character: char| {
                        !(character.is_ascii_uppercase()
                            || character.is_ascii_digit()
                            || character == '_')
                    }),
                "provider identity secret_ref must be an env: reference"
            );
        }

        Ok(Self {
            provider_id: raw.provider_id.clone(),
            display_name: raw.display_name.clone(),
            backend: raw.backend.clone(),
            scope: raw.scope.clone(),
            storage_domain: raw.storage_domain.clone(),
            credential_revision: raw.credential_revision,
            endpoint_revision: raw.endpoint_revision,
            secret_ref: raw.secret_ref.clone(),
            api_profile: raw.api_profile.clone(),
            strategy: raw.strategy.clone(),
            retired: raw.retired,
            cleanup: raw.cleanup,
        })
    }

    pub(crate) fn legacy(
        config_name: &str,
        backend: &str,
        secret_ref: Option<String>,
        api_profile: &str,
        strategy: &str,
    ) -> Self {
        Self {
            provider_id: format!("legacy:{config_name}"),
            display_name: config_name.to_owned(),
            backend: backend.to_owned(),
            scope: format!("legacy:{config_name}"),
            storage_domain: format!("legacy:{config_name}"),
            credential_revision: 1,
            endpoint_revision: 1,
            secret_ref,
            api_profile: api_profile.to_owned(),
            strategy: strategy.to_owned(),
            retired: false,
            cleanup: CleanupMode::Managed,
        }
    }

    pub fn route_snapshot(&self) -> ProviderRouteSnapshot {
        ProviderRouteSnapshot {
            provider_id: self.provider_id.clone(),
            backend: self.backend.clone(),
            scope: self.scope.clone(),
            storage_domain: self.storage_domain.clone(),
            credential_revision: self.credential_revision,
            endpoint_revision: self.endpoint_revision,
            secret_ref: self.secret_ref.clone(),
            api_profile: self.api_profile.clone(),
            strategy: self.strategy.clone(),
            cleanup: self.cleanup,
        }
    }

    pub fn validate_route_snapshot(&self, snapshot: &ProviderRouteSnapshot) -> anyhow::Result<()> {
        if self.route_snapshot() != *snapshot {
            bail!("provider route snapshot does not match the configured identity revision");
        }
        Ok(())
    }

    pub fn resource_key(
        &self,
        resource_type: RemoteResourceType,
        resource_id: impl Into<String>,
    ) -> CanonicalResourceKey {
        CanonicalResourceKey {
            backend: self.backend.clone(),
            scope: self.scope.clone(),
            resource_type,
            resource_id: resource_id.into(),
        }
    }
}

fn validate_identifier(field: &str, value: &str) -> anyhow::Result<()> {
    let valid = !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'-' | b'_' | b'.' | b':')
        });
    ensure!(
        valid,
        "provider identity {field} must be a non-empty lowercase canonical identifier"
    );
    Ok(())
}

pub(crate) fn validate_storage_domain(value: &str) -> anyhow::Result<()> {
    validate_identifier("primary_storage_domain", value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_snapshot_round_trip_contains_only_routing_references() {
        let identity = ProviderIdentity {
            provider_id: "pinata-prod".to_owned(),
            display_name: "Primary".to_owned(),
            backend: "pinata".to_owned(),
            scope: "account:prod".to_owned(),
            storage_domain: "pinata:prod".to_owned(),
            credential_revision: 2,
            endpoint_revision: 3,
            secret_ref: Some("env:PINATA_TOKEN".to_owned()),
            api_profile: "pinata-v3".to_owned(),
            strategy: "cid".to_owned(),
            retired: false,
            cleanup: CleanupMode::Retain,
        };

        let json = serde_json::to_string(&identity.route_snapshot()).unwrap();
        let decoded: ProviderRouteSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, identity.route_snapshot());
        assert!(!json.contains("display_name"));
        assert!(!json.contains("retired"));
        assert!(!json.contains("endpoint_url"));
        assert!(!json.contains("secret_hash"));
    }
}
