//! A declaration may bind unknown legacy scope; it cannot overwrite known facts.
use super::{Namespace, TableImage, model::field, refused};
use crate::{
    error::AppResult,
    pinning::{
        identity::{ProviderRouteSnapshot, RemoteResourceType},
        provider::ObservedResource,
    },
    store::pinning::ledger::submission::ResourceEvidence,
};
use serde_json::Value;
use std::collections::BTreeSet;

fn member(provider: Option<&str>, ns: &Namespace) -> bool {
    provider.is_some_and(|p| p == ns.source || p == ns.target)
}

fn profile_type(api: &str) -> AppResult<RemoteResourceType> {
    Ok(match api {
        "pinata-v3" | "pinata-legacy" | "pinata_v3" | "pinata_legacy" => {
            RemoteResourceType::HostedFile
        }
        "rpc" | "kubo" | "filebase-rpc" => RemoteResourceType::RpcPin,
        "cluster" | "cluster-proxy" | "cluster-rest" => RemoteResourceType::ClusterPin,
        "noop" | "psa" | "pinata-psa" | "filebase-psa" => RemoteResourceType::PsaRequest,
        _ => {
            return Err(refused(
                "historical API resource semantics cannot be explained",
            ));
        }
    })
}

fn facts(
    ns: &Namespace,
    backend: &str,
    scope: &str,
    kind: Option<RemoteResourceType>,
) -> AppResult<()> {
    if backend.is_empty()
        || scope.is_empty()
        || backend != ns.backend
        || (!scope.starts_with("legacy:") && scope != ns.scope)
        || kind.is_some_and(|kind| kind != ns.resource_type)
    {
        return Err(refused(
            "historical backend/resource type/account contradicts declaration",
        ));
    }
    Ok(())
}

fn route(ns: &Namespace, raw: &str) -> AppResult<()> {
    let route: ProviderRouteSnapshot =
        serde_json::from_str(raw).map_err(|_| refused("historical route cannot be explained"))?;
    facts(
        ns,
        &route.backend,
        &route.scope,
        Some(profile_type(&route.api_profile)?),
    )
}

pub(super) fn typed_route(ns: &Namespace, route: &ProviderRouteSnapshot) -> AppResult<()> {
    facts(
        ns,
        &route.backend,
        &route.scope,
        Some(profile_type(&route.api_profile)?),
    )
}

fn resource(ns: &Namespace, value: &ObservedResource) -> AppResult<()> {
    if value.resource_type != ns.resource_type {
        return Err(refused("reported resource type contradicts declaration"));
    }
    if let Some(replica) = &value.replica_evidence {
        facts(
            ns,
            &replica.resource.pinset.backend,
            &replica.resource.pinset.scope,
            Some(RemoteResourceType::ClusterPin),
        )?;
    }
    Ok(())
}

pub(super) fn validate(images: &[TableImage], ns: &Namespace) -> AppResult<()> {
    let mut jobs = BTreeSet::new();
    for image in images {
        for row in &image.rows {
            if member(field(row, "provider"), ns) {
                if image.table == "pin_jobs"
                    && let Some(id) = field(row, "id")
                {
                    jobs.insert(id.to_owned());
                }
                if matches!(
                    image.table.as_str(),
                    "pin_submit_observations" | "pin_replica_observations"
                ) && let Some(id) = field(row, "job_id")
                {
                    jobs.insert(id.to_owned());
                }
                if let Some(raw) = field(row, "route").or_else(|| {
                    (image.table == "pin_provider_routes")
                        .then(|| field(row, "snapshot"))
                        .flatten()
                }) {
                    route(ns, raw)?;
                }
                if image.table == "pin_resource_history" {
                    let raw = field(row, "ledger")
                        .ok_or_else(|| refused("historical release image missing"))?;
                    captured(ns, &json(raw)?, true, 0)?;
                }
                if image.table == "pin_submit_observations" {
                    let resources: Vec<ResourceEvidence> = serde_json::from_str(
                        field(row, "resources").ok_or_else(|| refused("resource image missing"))?,
                    )
                    .map_err(|_| refused("historical resources cannot be explained"))?;
                    for value in resources {
                        resource(ns, &value.resource)?;
                        if let Some(key) = value.key {
                            facts(ns, &key.backend, &key.scope, Some(key.resource_type))?;
                        }
                    }
                }
                if image.table == "pin_replica_observations" {
                    let resources: Vec<ObservedResource> = serde_json::from_str(
                        field(row, "resources").ok_or_else(|| refused("replica image missing"))?,
                    )
                    .map_err(|_| refused("historical replicas cannot be explained"))?;
                    for value in resources {
                        resource(ns, &value)?;
                    }
                }
            }
            if super::projection::CAPTURES.contains(&image.table.as_str()) {
                for column in super::projection::CAPTURE_COLUMNS {
                    if let Some(raw) = field(row, column) {
                        captured(ns, &json(raw)?, false, 0)?;
                    }
                }
            }
        }
    }
    // Captures and terminal histories are associated through original job IDs,
    // including detached observations whose owner/job no longer exists.
    for image in images {
        for row in &image.rows {
            if field(row, "job_id").is_some_and(|id| jobs.contains(id)) {
                if image.table == "pin_invocation_routes" {
                    route(
                        ns,
                        field(row, "route").ok_or_else(|| refused("invocation route missing"))?,
                    )?;
                }
                if image.table == "pin_submit_history"
                    && profile_type(
                        field(row, "api").ok_or_else(|| refused("historical API missing"))?,
                    )? != ns.resource_type
                {
                    return Err(refused(
                        "actual historical API contradicts declaration resource type",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn json(raw: &str) -> AppResult<Value> {
    serde_json::from_str(raw).map_err(|_| refused("historical identity JSON cannot be explained"))
}

fn captured(ns: &Namespace, value: &Value, associated: bool, depth: usize) -> AppResult<()> {
    if depth > 32 {
        return Err(refused("historical identity JSON exceeds depth budget"));
    }
    match value {
        Value::Object(values) => {
            let associated = associated
                || member(values.get("provider").and_then(Value::as_str), ns)
                || member(values.get("name").and_then(Value::as_str), ns)
                || values
                    .get("provider_id")
                    .and_then(Value::as_str)
                    .is_some_and(|p| p == format!("legacy:{}", ns.source));
            if associated {
                if let (Some(backend), Some(scope)) = (values.get("backend"), values.get("scope")) {
                    let backend = backend
                        .as_str()
                        .ok_or_else(|| refused("historical backend invalid"))?;
                    let scope = scope
                        .as_str()
                        .ok_or_else(|| refused("historical account invalid"))?;
                    facts(ns, backend, scope, None)?;
                    if let Some(api) = values.get("api_profile") {
                        facts(
                            ns,
                            backend,
                            scope,
                            Some(profile_type(
                                api.as_str()
                                    .ok_or_else(|| refused("historical API invalid"))?,
                            )?),
                        )?;
                    }
                    if let Some(kind) = values.get("resource_type") {
                        facts(
                            ns,
                            backend,
                            scope,
                            Some(
                                serde_json::from_value(kind.clone())
                                    .map_err(|_| refused("historical resource type invalid"))?,
                            ),
                        )?;
                    }
                }
                if let Some(provider) = values
                    .get("provider")
                    .and_then(Value::as_str)
                    .and_then(|p| p.strip_prefix("domain:"))
                {
                    let (backend, scope, kind): (String, String, RemoteResourceType) =
                        serde_json::from_str(provider)
                            .map_err(|_| refused("historical domain key invalid"))?;
                    facts(ns, &backend, &scope, Some(kind))?;
                }
            }
            for (key, value) in values {
                if associated
                    && key == "route"
                    && let Some(raw) = value.as_str()
                {
                    route(ns, raw)?;
                } else {
                    captured(
                        ns,
                        value,
                        associated || key == &ns.source || key == &ns.target,
                        depth + 1,
                    )?;
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                captured(ns, value, associated, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}
