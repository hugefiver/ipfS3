//! Local, read-only configuration evidence. Never construct AppState here: it
//! connects to (and migrates) the database and starts provider-related state.
use std::{collections::BTreeMap, path::Path};

use anyhow::{Context as _, anyhow};
use chrono::Utc;
use ipfs_s3_gateway::{
    config::Config,
    pinning::config::{
        PinataStrategy, PolicyTrigger, ProviderKind, ProviderMode, ValidatedPinningConfig,
        ValidatedProvider,
    },
    pinning::coordinator::normalize_validated_config,
};
use serde_json::{Value, json};

#[path = "diagnostics/status.rs"]
mod status;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Doctor,
    ConfigExplain,
    PolicyExplain {
        bucket: String,
        key: String,
    },
    Status {
        bucket: String,
        key: String,
        version_id: Option<String>,
        cursor: Option<String>,
    },
}

fn local_config() -> anyhow::Result<(Config, Value)> {
    // Only emit provenance labels, never the path, env values, URL or parser
    // errors: each of those may contain arbitrary credentials or query data.
    let file_present =
        Path::new(&std::env::var("IPFS_S3_CONFIG").unwrap_or_else(|_| "config.toml".to_owned()))
            .exists();
    let env = |name: &str| std::env::var(name).ok();
    let credential_override = env("IPFS_S3_ACCESS_KEY_ID").is_some_and(|value| !value.is_empty())
        && env("IPFS_S3_SECRET_ACCESS_KEY").is_some_and(|value| !value.is_empty());
    let source = |overridden| {
        if overridden {
            "environment"
        } else if file_present {
            "file_or_embedded_default"
        } else {
            "embedded_default"
        }
    };
    let provenance = json!({
        "evidence": "independent_local_cli_load_not_running_gateway",
        "running_gateway_configuration": "not_observed",
        "gateway_startup_checks": "not_performed",
        "config_file": if file_present { "present" } else { "missing" },
        "config_path_override": if env("IPFS_S3_CONFIG").is_some() { "present" } else { "missing" },
        "effective_sources": {
            "pinning": source(false),
            "pinning_identity": source(false),
            "auth_credentials": source(credential_override),
            "master_key": source(env("IPFS_S3_MASTER_KEY").is_some_and(|value| !value.is_empty())),
            "database_url": source(env("IPFS_S3_DATABASE_URL").is_some()),
            "kubo_rpc_url": source(env("IPFS_S3_KUBO_RPC_URL").is_some()),
        },
        "variables": {
            "access_key_id": presence(env("IPFS_S3_ACCESS_KEY_ID").as_deref()),
            "secret_access_key": presence(env("IPFS_S3_SECRET_ACCESS_KEY").as_deref()),
            "master_key": presence(env("IPFS_S3_MASTER_KEY").as_deref()),
        }
    });
    // The same loader as gateway startup; never return
    // free-form errors from config files or provider names to the CLI.
    let config = Config::load()
        .map_err(|_| anyhow!("local configuration load failed (details withheld)"))?;
    Ok((config, provenance))
}

fn presence(value: Option<&str>) -> &'static str {
    if value.is_some_and(|value| !value.is_empty()) {
        "present"
    } else {
        "missing"
    }
}

fn provider_token_variables(config: &Config) -> Vec<Value> {
    config
        .pinning
        .providers
        .iter()
        .enumerate()
        .map(|(index, provider)| {
            json!({
                "provider_index": index,
                "token_env_reference": if provider.token_env.is_some() { "configured" } else { "missing" },
                "token_env_value": provider.token_env.as_deref().map_or("not_configured", |name| {
                    presence(std::env::var(name).ok().as_deref())
                }),
            })
        })
        .collect()
}

fn capability(operation: &'static str, configured: &'static str) -> Value {
    json!({
        "operation": operation,
        "configured_route": configured,
        "remote_permission": "unknown",
        "remote_observation": "not_performed"
    })
}

fn provider_operations(provider: &ValidatedProvider) -> Value {
    let active = provider.limits.enabled;
    let kind = provider.kind;
    let route = provider.identity.strategy.as_str();
    let supported = |operation: &'static str, route_allowed: bool| {
        capability(
            operation,
            if !active || kind == ProviderKind::Noop || !route_allowed {
                "denied"
            } else {
                "supported"
            },
        )
    };
    let cleanup =
        if provider.identity.cleanup == ipfs_s3_gateway::pinning::identity::CleanupMode::Retain {
            "denied"
        } else if active && kind != ProviderKind::Noop {
            "unknown"
        } else {
            "denied"
        };
    json!([
        supported("submit_cid", route == "cid"),
        supported("upload_bytes", route == "upload"),
        supported("query_remote", true),
        capability("remove_remote", cleanup),
    ])
}

fn providers(validated: &ValidatedPinningConfig, explicit_identity: bool) -> Vec<Value> {
    // A local ordinal represents each (backend, scope), without exposing a
    // possibly sensitive user-configured scope string or hashing it.
    let mut scopes = BTreeMap::<(String, String), usize>::new();
    validated
        .providers
        .iter()
        .enumerate()
        .map(|(index, provider)| {
            let identity = &provider.identity;
            let next = scopes.len() + 1;
            let scope = *scopes
                .entry((identity.backend.clone(), identity.scope.clone()))
                .or_insert(next);
            let route = match (provider.kind, provider.pinata.as_ref()) {
                (ProviderKind::Pinata, Some(options)) if options.strategy == PinataStrategy::Upload => "upload",
                (ProviderKind::Noop, _) => "noop",
                _ => "cid",
            };
            json!({
                "provider_index": index,
                "scope_ref": scope,
                "scope_evidence": if explicit_identity { "explicit_identity" } else { "legacy_config_name" },
                "backend": match provider.kind { ProviderKind::Pinata => "pinata", ProviderKind::Filebase => "filebase", ProviderKind::Noop => "noop" },
                "credential_revision": identity.credential_revision,
                "endpoint_revision": identity.endpoint_revision,
                "enabled": provider.limits.enabled,
                "retired": identity.retired,
                "credential": if provider.token.is_some() { "present" } else if provider.kind == ProviderKind::Noop { "not_required" } else { "missing" },
                "strategy": route,
                "operations": provider_operations(provider),
            })
        })
        .collect()
}

fn explain_policy(validated: &ValidatedPinningConfig, bucket: &str, key: &str) -> Value {
    let matched = validated.policies.iter().enumerate().find(|(_, policy)| {
        (policy.bucket == "*" || policy.bucket == bucket) && key.starts_with(&policy.prefix)
    });
    let Some((index, policy)) = matched else {
        return json!({
            "match": "none",
            "scope_ref": "requested_object_not_printed",
            "config_revision": "unknown",
            "operations": [capability("automatic_pin", "denied"), capability("manual_pin", "denied")],
            "remote_permission": "unknown"
        });
    };
    let active = policy
        .providers
        .iter()
        .filter(|name| {
            validated.providers.iter().any(|provider| {
                &provider.name == *name
                    && provider.limits.enabled
                    && provider.kind != ProviderKind::Noop
            })
        })
        .count();
    let pin_capability = if active == 0
        || (policy.provider_mode == ProviderMode::All && active != policy.providers.len())
    {
        "denied"
    } else {
        "supported"
    };
    json!({
        "match": "first_matching_rule",
        "scope_ref": "requested_object_not_printed",
        "policy_index": index,
        // ValidatedPolicy.identity is a SHA digest; do not emit it.
        "config_revision": "unknown",
        "provider_mode": if policy.provider_mode == ProviderMode::All { "all" } else { "one" },
        "provider_indices": policy.providers.iter().filter_map(|name| validated.providers.iter().position(|provider| &provider.name == name)).collect::<Vec<_>>(),
        "allow_decompressed": policy.allow_decompressed,
        "operations": [
            capability("automatic_pin", if policy.trigger == PolicyTrigger::Always { pin_capability } else { "denied" }),
            capability("manual_pin", pin_capability),
            capability("decompressed_pin", if policy.allow_decompressed { pin_capability } else { "denied" }),
        ],
        "caveat": "local_rule_match_only_no_tags_or_object_version_or_remote_permissions_checked"
    })
}

pub async fn run(command: Command) -> anyhow::Result<()> {
    let (cfg, mut provenance) = local_config()?;
    if let Command::Status {
        bucket,
        key,
        version_id,
        cursor,
    } = &command
    {
        let result = status::snapshot(
            &cfg.storage.database_url,
            bucket,
            key,
            version_id.as_deref(),
            cursor.as_deref(),
        )
        .await?;
        return print_report(&json!({
            "command": "pinning_status",
            "access_scope": "local_administrator_cli_no_http_endpoint",
            "build_version": env!("CARGO_PKG_VERSION"),
            "observed_at": Utc::now().to_rfc3339(),
            "provenance": provenance,
            "status": result,
            "side_effects": "read_only_database_snapshot_no_remote_probe_no_migrations",
        }));
    }
    provenance["variables"]["provider_tokens"] = json!(provider_token_variables(&cfg));
    provenance["gateway_startup_checks"] = json!("pinning_config_only_no_full_startup");
    let validation = ValidatedPinningConfig::from_config(&cfg, |name| std::env::var(name).ok())
        .and_then(normalize_validated_config);
    let command_name = match &command {
        Command::Doctor => "pinning_doctor",
        Command::ConfigExplain => "config_explain",
        Command::PolicyExplain { .. } => "pinning_policy_explain",
        Command::Status { .. } => unreachable!("status is handled before config validation"),
    };
    let observed_at = Utc::now().to_rfc3339();
    let validated = match validation {
        Ok(validated) => validated,
        Err(_) => {
            let report = json!({
                "command": command_name,
                "build_version": env!("CARGO_PKG_VERSION"),
                "observed_at": observed_at,
                "provenance": provenance,
                "pinning": {
                    "validation": "failed",
                    "configured_providers": cfg.pinning.providers.len(),
                    "configured_policies": cfg.pinning.policies.len(),
                    "config_revision": "unknown",
                    "remote_capabilities": "unknown_not_probed",
                    "database_status": "not_observed",
                },
                "side_effects": "no_database_connection_no_remote_probe_no_upload",
            });
            print_report(&report)?;
            return Err(anyhow!(
                "pinning configuration validation failed (details withheld)"
            ));
        }
    };
    let mut report = json!({
        "command": command_name,
        "build_version": env!("CARGO_PKG_VERSION"),
        "observed_at": observed_at,
        "provenance": provenance,
        "pinning": {
            "validation": "passed",
            "configured_providers": validated.providers.len(),
            "configured_policies": validated.policies.len(),
            "providers": providers(&validated, !cfg.pinning_identity.providers.is_empty()),
            "config_revision": "unknown",
            "remote_capabilities": "unknown_not_probed",
            "database_status": "not_observed",
        },
        "side_effects": "no_database_connection_no_remote_probe_no_upload",
    });
    if let Command::PolicyExplain { bucket, key } = command {
        report["policy_explain"] = explain_policy(&validated, &bucket, &key);
    }
    print_report(&report)
}

fn print_report(report: &Value) -> anyhow::Result<()> {
    // JSON serialization here contains only allowlisted values. Avoid debug
    // formatting the original configuration or errors in this code path.
    let output =
        serde_json::to_string_pretty(report).context("diagnostic report serialization failed")?;
    println!("{output}");
    Ok(())
}
