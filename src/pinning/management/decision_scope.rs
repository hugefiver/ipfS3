//! Decision-local disjointness; the native inventory is never narrowed here.
use std::collections::BTreeSet;

use super::{capture, model::*, validate::validate_route_identity};
use crate::{pinning::identity::ProviderRouteSnapshot, store::pinning::management::native::{NativeFrame, value}};

type Domain = (String, String, ResourceType);

fn domain(route: &RouteIdentity) -> Option<Domain> {
    let original = route.original.as_ref()?;
    if RouteIdentity::from_original(original.clone()) != *route
        || validate_route_identity(route).is_err()
        || original.provider_id.starts_with("legacy:")
        || !matches!(original.api_profile.as_str(), "noop" | "psa" | "pinata-v3" | "pinata-legacy"
            | "rpc" | "kubo" | "filebase-rpc" | "cluster" | "cluster-proxy" | "cluster-rest")
    {
        return None;
    }
    Some((route.backend.clone(), route.scope.clone(), route.resource_type))
}

pub(crate) struct DecisionScope {
    cid: Option<String>,
    domains: BTreeSet<Domain>,
    complete: bool,
}

impl DecisionScope {
    pub(crate) fn for_item(item: &ItemRequest, snapshot: &Snapshot) -> Self {
        let mut scope = Self { cid: None, domains: BTreeSet::new(), complete: true };
        let (selected, target) = match &item.subject {
            Subject::Provider { route } => (Some(route), None),
            Subject::Object { identity, target } => {
                scope.cid = crate::pinning::provider::canonical_resource_cid(&identity.cid).ok();
                match target {
                    TargetSelection::New(route) => (Some(route), None),
                    TargetSelection::Existing { target_id, .. } => {
                        let target = snapshot.targets.iter().find(|target| target.id == *target_id);
                        if let Some(target) = target {
                            scope.cid = crate::pinning::provider::canonical_resource_cid(&target.cid).ok();
                        }
                        (target.and_then(|target| target.route.as_ref()), target)
                    }
                }
            }
        };
        scope.add_registered(selected, snapshot);
        if let Action::Migrate { destination } = &item.action {
            scope.add_registered(Some(destination), snapshot);
        }
        if let Some(target) = target {
            // One and All both retain the entire birth candidate universe. An
            // unknown slot cannot be assigned to today's configured account.
            for candidate in &target.frozen_candidates {
                scope.add_registered(candidate.route.as_ref(), snapshot);
            }
            if target.frozen_candidates.is_empty() && target.requirements.is_none() {
                scope.complete = false;
            }
        }
        if matches!(item.subject, Subject::Object { .. }) {
            match capture::requirement_routes(item, target, snapshot) {
                Ok(routes) => {
                    for route in routes {
                        scope.add_registered(Some(&route.identity), snapshot);
                    }
                }
                Err(_) => scope.complete = false,
            }
        }
        scope
    }

    fn add_registered(&mut self, identity: Option<&RouteIdentity>, snapshot: &Snapshot) {
        let proved = identity.and_then(|identity| {
            snapshot.routes.iter().find(|route| route.identity == *identity
                && route.original.as_ref() == identity.original.as_ref()).and_then(|route| domain(&route.identity))
        });
        if let Some(domain) = proved { self.domains.insert(domain); }
        else { self.complete = false; }
    }

    pub(crate) fn includes(&self, key: &RemoteKey) -> bool {
        !self.complete || self.domains.contains(&(key.backend.clone(), key.scope.clone(), key.resource_type))
    }

    pub(crate) fn proves_disjoint(&self, frame: &NativeFrame, key: &NativeKey) -> bool {
        let values = key.values.iter().map(String::as_str).collect::<Vec<_>>();
        let Some(row) = frame.row(&key.table, &values) else { return false; };
        let Some(cid) = value(row, "cid").and_then(|cid| crate::pinning::provider::canonical_resource_cid(cid).ok()) else { return false; };
        let Some(selected) = &self.cid else { return false; };
        if cid != *selected { return true; }
        resource_route(frame, key).and_then(|original| domain(&RouteIdentity::from_original(original)))
            .is_some_and(|domain| self.complete && !self.domains.contains(&domain))
    }
}

fn decode_route(json: &str) -> Option<ProviderRouteSnapshot> {
    let route: ProviderRouteSnapshot = serde_json::from_str(json).ok()?;
    domain(&RouteIdentity::from_original(route.clone()))?;
    Some(route)
}

fn history_route(frame: &NativeFrame, provider: &str, cid: &str, epoch: &str) -> Option<ProviderRouteSnapshot> {
    let json = frame.original_value("pin_resource_history", &[provider, cid, epoch], "ledger")?;
    let envelope: serde_json::Value = serde_json::from_str(json).ok()?;
    let ledger = envelope.get("ledger")?;
    if ledger.get("provider")?.as_str()? != provider || ledger.get("cid")?.as_str()? != cid { return None; }
    decode_route(ledger.get("route")?.as_str()?)
}

/// Missing evidence may use an exact trusted R26 point. Malformed or conflicting
/// declarations must not be repaired by substituting another routing contract.
pub(crate) fn resource_route_declared(frame: &NativeFrame, key: &NativeKey) -> bool {
    let [provider, cid, rest @ ..] = key.values.as_slice() else { return true; };
    if key.table != "pin_resource_history" {
        return frame.original_value("remote_pin_ledger", &[provider, cid], "route").is_some();
    }
    let [epoch] = rest else { return true; };
    let Some(json) = frame.original_value("pin_resource_history", &[provider, cid, epoch], "ledger") else { return false; };
    let Ok(envelope) = serde_json::from_str::<serde_json::Value>(json) else { return true; };
    let Some(ledger) = envelope.get("ledger") else { return true; };
    if ledger.get("provider").and_then(serde_json::Value::as_str) != Some(provider.as_str())
        || ledger.get("cid").and_then(serde_json::Value::as_str) != Some(cid.as_str())
    { return true; }
    ledger.get("route").is_some_and(|route| !route.is_null())
}

/// Only the exact persisted invocation contract proves an allocation domain.
/// Provider aliases and configuration names are never account evidence.
pub(crate) fn resource_route(frame: &NativeFrame, key: &NativeKey) -> Option<ProviderRouteSnapshot> {
    let [provider, cid, rest @ ..] = key.values.as_slice() else { return None; };
    if key.table == "pin_resource_history" {
        let [epoch] = rest else { return None; };
        return history_route(frame, provider, cid, epoch);
    }
    if !matches!(key.table.as_str(), "remote_pins" | "remote_pin_ledger") || !rest.is_empty() { return None; }
    let original = decode_route(frame.original_value("remote_pin_ledger", &[provider, cid], "route")?)?;
    if let Some(remote) = frame.row("remote_pins", &[provider, cid]) {
        let epoch = value(remote, "epoch")?;
        if frame.row("pin_resource_history", &[provider, cid, epoch]).is_some()
            && history_route(frame, provider, cid, epoch).as_ref() != Some(&original)
        {
            return None;
        }
    }
    Some(original)
}
