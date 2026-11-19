//! Local, read-only configuration evidence. Never construct AppState here: it
//! connects to (and migrates) the database and starts provider-related state.
use std::{collections::BTreeMap, path::Path};

use anyhow::{Context as _, anyhow};
use chrono::Utc;
use ipfs_s3_gateway::{
    config::Config,
    pinning::{
        config::{ProviderKind, ValidatedPinningConfig, ValidatedProvider},
        coordinator::normalize_validated_config,
    },
};
use serde_json::{Value, json};

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
        "gateway_startup_checks": "pinning_config_only_no_full_startup",
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
            if let Some(cluster) = &provider.cluster {
                let credentials = |auth: &ipfs_s3_gateway::pinning::cluster::config::ClusterCredentialConfig| {
                    let (token_reference, token_value) = credential_variable(auth.token_env.as_deref(), auth.auth != "none");
                    let (username_reference, username_value) = credential_variable(auth.username_env.as_deref(), auth.auth == "basic");
                    json!({"token_env_reference": token_reference, "token_env_value": token_value,
                        "username_env_reference": username_reference, "username_env_value": username_value})
                };
                return json!({"provider_index": index, "proxy": credentials(&cluster.proxy_auth),
                    "rest": credentials(&cluster.rest_auth), "source": credentials(&cluster.source)});
            }
            let rpc = config
                .pinning_rpc
                .providers
                .iter()
                .find(|rpc| rpc.config_name == provider.name);
            let token_required = rpc.map_or(provider.kind != "noop", |rpc| rpc.auth != "none");
            let (token_reference, token_value) =
                credential_variable(provider.token_env.as_deref(), token_required);
            let (username_reference, username_value) = credential_variable(
                rpc.and_then(|rpc| rpc.username_env.as_deref()),
                rpc.is_some_and(|rpc| rpc.auth == "basic"),
            );
            json!({
                "provider_index": index,
                "token_env_reference": token_reference,
                "token_env_value": token_value,
                "username_env_reference": username_reference,
                "username_env_value": username_value,
            })
        })
        .collect()
}

fn credential_variable(name: Option<&str>, required: bool) -> (&'static str, &'static str) {
    match name.filter(|name| !name.is_empty()) {
        Some(name) => ("configured", presence(std::env::var(name).ok().as_deref())),
        None if required => ("missing", "missing"),
        None => ("not_required", "not_required"),
    }
}

fn capability(operation: &'static str, configured: &'static str) -> Value {
    json!({
        "operation": operation,
        "configured_route": configured,
        "remote_permission": "unknown",
        "remote_observation": "not_performed"
    })
}

fn provider_strategy(provider: &ValidatedProvider) -> &'static str {
    if provider.kind == ProviderKind::Noop {
        return "noop";
    }
    match provider.identity.strategy.as_str() {
        "cid" => "cid",
        "upload" => "upload",
        "car" => "car",
        _ => "unknown",
    }
}

fn provider_profile(provider: &ValidatedProvider) -> &'static str {
    match provider.identity.api_profile.as_str() {
        "pinata-v3" => "pinata-v3",
        "pinata-legacy" => "pinata-legacy",
        "pinata-psa" => "pinata-psa",
        "filebase-psa" => "filebase-psa",
        "filebase-rpc" => "filebase-rpc",
        "kubo" => "kubo",
        "cluster-proxy" => "cluster-proxy",
        "cluster-rest" => "cluster-rest",
        "noop" => "noop",
        _ => "unknown",
    }
}

fn provider_credential(provider: &ValidatedProvider) -> &'static str {
    if let Some(cluster) = &provider.cluster {
        let auth = match cluster.protocol {
            ipfs_s3_gateway::pinning::cluster::provider::ClusterProtocol::Proxy => {
                &cluster.proxy_auth
            }
            ipfs_s3_gateway::pinning::cluster::provider::ClusterProtocol::Rest => {
                &cluster.rest_auth
            }
        };
        return if auth.is_some() {
            "present"
        } else {
            "not_required"
        };
    }
    if let Some(rpc) = &provider.rpc {
        // Basic and Bearer credentials were checked by pure config validation;
        // presence says nothing about remote authorization. Anonymous RPC does
        // not require a token. Never inspect the credential payload here.
        if rpc.auth.is_some() {
            "present"
        } else {
            "not_required"
        }
    } else if provider.token.is_some() {
        "present"
    } else if provider.kind == ProviderKind::Noop {
        "not_required"
    } else {
        "missing"
    }
}

fn provider_operations(provider: &ValidatedProvider, strategy: &str) -> Value {
    let active = provider.limits.enabled && provider.kind != ProviderKind::Noop;
    let configured = |operation, route_allowed| {
        capability(
            operation,
            if active && route_allowed {
                "supported"
            } else {
                "denied"
            },
        )
    };
    json!([
        configured("submit_cid", strategy == "cid"),
        configured("upload_bytes", strategy == "upload"),
        configured("import_car", strategy == "car"),
        configured("remove_remote", true),
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
            let strategy = provider_strategy(provider);
            let mut report = json!({
                "provider_index": index,
                "scope_ref": scope,
                "scope_evidence": if explicit_identity { "explicit_identity" } else { "legacy_config_name" },
                "backend": match provider.kind { ProviderKind::Pinata => "pinata", ProviderKind::Filebase => "filebase", ProviderKind::IpfsRpc => "kubo", ProviderKind::Cluster => "cluster", ProviderKind::Noop => "noop" },
                "profile": provider_profile(provider),
                "credential_revision": identity.credential_revision,
                "endpoint_revision": identity.endpoint_revision,
                "enabled": provider.limits.enabled,
                "retired": identity.retired,
                "credential": provider_credential(provider),
                "strategy": strategy,
                "operations": provider_operations(provider, strategy),
            });
            if let Some(cluster) = &provider.cluster {
                report["route_credentials"] = json!({
                    "proxy": if cluster.proxy_auth.is_some() { "present" } else { "not_required" },
                    "rest": if cluster.rest_auth.is_some() { "present" } else { "not_required" },
                    "source": if cluster.source.as_ref().is_some_and(|s| s.auth.is_some()) { "present" } else { "not_required" },
                    "evidence": "local_configuration_only_not_account_authorization",
                });
            }
            report
        })
        .collect()
}

pub fn config_explain() -> anyhow::Result<()> {
    let (cfg, mut provenance) = local_config()?;
    provenance["variables"]["provider_tokens"] = json!(provider_token_variables(&cfg));
    let validation = ValidatedPinningConfig::from_config(&cfg, |name| std::env::var(name).ok())
        .and_then(normalize_validated_config);
    let observed_at = Utc::now().to_rfc3339();
    let validated = match validation {
        Ok(validated) => validated,
        Err(_) => {
            let report = json!({
                "command": "config_explain",
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
    print_report(&json!({
        "command": "config_explain",
        "build_version": env!("CARGO_PKG_VERSION"),
        "observed_at": observed_at,
        "provenance": provenance,
        "pinning": {
            "validation": "passed",
            "configured_providers": validated.providers.len(),
            "configured_policies": validated.policies.len(),
            "providers": providers(&validated, !cfg.pinning_identity.providers.is_empty()),
            "worker_completion": "provider_ack",
            "physical_replicas": "not_observed",
            "config_revision": "unknown",
            "remote_capabilities": "unknown_not_probed",
            "database_status": "not_observed",
        },
        "side_effects": "no_database_connection_no_remote_probe_no_upload",
    }))
}

fn print_report(report: &Value) -> anyhow::Result<()> {
    // JSON serialization here contains only allowlisted values. Avoid debug
    // formatting the original configuration or errors in this code path.
    let output =
        serde_json::to_string_pretty(report).context("diagnostic report serialization failed")?;
    println!("{output}");
    Ok(())
}
