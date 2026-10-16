//! Decision-local disjointness; the native inventory is never narrowed here.
use std::collections::BTreeSet;

use super::{capture, model::*, validate::validate_route_identity};
use crate::store::pinning::management::records::SharedDecodeBudget;
use crate::{
    pinning::identity::ProviderRouteSnapshot,
    store::pinning::management::native::{NativeFrame, value},
};

type Domain = (String, String, ResourceType);

fn domain(route: &RouteIdentity) -> Option<Domain> {
    let original = route.original.as_ref()?;
    if RouteIdentity::from_original(original.clone()) != *route
        || validate_route_identity(route).is_err()
        || original.provider_id.starts_with("legacy:")
        || !matches!(
            original.api_profile.as_str(),
            "noop"
                | "psa"
                | "pinata-psa"
                | "filebase-psa"
                | "pinata-v3"
                | "pinata-legacy"
                | "rpc"
                | "kubo"
                | "filebase-rpc"
                | "cluster"
                | "cluster-proxy"
                | "cluster-rest"
        )
    {
        return None;
    }
    Some((
        route.backend.clone(),
        route.scope.clone(),
        route.resource_type,
    ))
}

pub(crate) struct DecisionScope {
    cid: Option<String>,
    domains: BTreeSet<Domain>,
    complete: bool,
}

impl DecisionScope {
    pub(crate) fn for_item(item: &ItemRequest, snapshot: &Snapshot) -> Self {
        Self::for_view(item, &snapshot.into())
    }
    pub(crate) fn for_view(item: &ItemRequest, snapshot: &super::SnapshotView<'_>) -> Self {
        let mut scope = Self {
            cid: None,
            domains: BTreeSet::new(),
            complete: true,
        };
        let (selected, target) = match &item.subject {
            Subject::Provider { route } => (Some(route), None),
            Subject::Object { identity, target } => {
                scope.cid = crate::pinning::provider::canonical_resource_cid(&identity.cid).ok();
                match target {
                    TargetSelection::New(route) => (Some(route), None),
                    TargetSelection::Existing { target_id, .. } => {
                        let target = snapshot
                            .targets
                            .iter()
                            .find(|target| target.id == *target_id);
                        if let Some(target) = target {
                            scope.cid =
                                crate::pinning::provider::canonical_resource_cid(&target.cid).ok();
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

    fn add_registered(
        &mut self,
        identity: Option<&RouteIdentity>,
        snapshot: &super::SnapshotView<'_>,
    ) {
        let proved = identity.and_then(|identity| {
            snapshot
                .routes
                .iter()
                .find(|route| {
                    route.identity == *identity
                        && route.original.as_ref() == identity.original.as_ref()
                })
                .and_then(|route| domain(&route.identity))
        });
        if let Some(domain) = proved {
            self.domains.insert(domain);
        } else {
            self.complete = false;
        }
    }

    pub(crate) fn includes(&self, key: &RemoteKey) -> bool {
        !self.complete
            || self.domains.iter().any(|domain| {
                domain.0 == key.backend && domain.1 == key.scope && domain.2 == key.resource_type
            })
    }

    pub(crate) fn proves_disjoint<'a>(
        &self,
        frame: impl Into<crate::store::pinning::management::records::FrozenRows<'a>>,
        key: &NativeKey,
        work: &SharedDecodeBudget,
    ) -> bool {
        let frame = frame.into();
        let Some(row) = frame.row_key(&key.table, &key.values) else {
            return false;
        };
        if work
            .lock()
            .ok()
            .and_then(|mut work| work.copy(&value(row, "cid")).ok())
            .is_none()
        {
            return false;
        }
        let Some(cid) = value(row, "cid")
            .and_then(|cid| crate::pinning::provider::canonical_resource_cid(cid).ok())
        else {
            return false;
        };
        let Some(selected) = &self.cid else {
            return false;
        };
        if cid != *selected {
            return true;
        }
        let Some(original) = resource_route(frame, key, work) else {
            return false;
        };
        let resource = match original.resource_type() {
            crate::pinning::identity::RemoteResourceType::PsaRequest => ResourceType::PsaRequest,
            crate::pinning::identity::RemoteResourceType::HostedFile => ResourceType::HostedFile,
            crate::pinning::identity::RemoteResourceType::RpcPin => ResourceType::RpcPin,
            crate::pinning::identity::RemoteResourceType::ClusterPin => ResourceType::ClusterPin,
        };
        self.complete
            && !self.domains.iter().any(|domain| {
                domain.0 == original.backend && domain.1 == original.scope && domain.2 == resource
            })
    }
}

fn decode_route(json: &str, work: &SharedDecodeBudget) -> Option<ProviderRouteSnapshot> {
    let mut work = work.lock().ok()?;
    let parsed = work.typed_shared::<ProviderRouteSnapshot>(json).ok()?;
    for _ in 0..3 {
        work.copy(parsed.as_ref()).ok()?;
    }
    for _ in 0..2 {
        work.copy(&(
            &parsed.provider_id,
            &parsed.backend,
            &parsed.scope,
            &parsed.storage_domain,
            &parsed.api_profile,
            &parsed.strategy,
        ))
        .ok()?;
    }
    work.copy(&(&parsed.backend, &parsed.scope)).ok()?;
    drop(work);
    let route = parsed.as_ref().clone();
    domain(&RouteIdentity::from_original(route.clone()))?;
    Some(route)
}

#[derive(serde::Deserialize)]
struct HistoryRoute {
    ledger: HistoryLedger,
}
#[derive(serde::Deserialize)]
struct HistoryLedger {
    provider: String,
    cid: String,
    route: Option<String>,
}
fn history_route(
    frame: crate::store::pinning::management::records::FrozenRows<'_>,
    provider: &str,
    cid: &str,
    epoch: &str,
    work: &SharedDecodeBudget,
) -> Option<ProviderRouteSnapshot> {
    let json = frame.original_value("pin_resource_history", &[provider, cid, epoch], "ledger")?;
    let envelope = work.lock().ok()?.typed_shared::<HistoryRoute>(json).ok()?;
    if envelope.ledger.provider != provider || envelope.ledger.cid != cid {
        return None;
    }
    decode_route(envelope.ledger.route.as_deref()?, work)
}

/// Missing evidence may use an exact trusted R26 point. Malformed or conflicting
/// declarations must not be repaired by substituting another routing contract.
pub(crate) fn resource_route_declared(
    frame: &NativeFrame,
    key: &NativeKey,
    work: &SharedDecodeBudget,
) -> bool {
    let [provider, cid, rest @ ..] = key.values.as_slice() else {
        return true;
    };
    if key.table != "pin_resource_history" {
        return frame
            .original_value("remote_pin_ledger", &[provider, cid], "route")
            .is_some();
    }
    let [epoch] = rest else {
        return true;
    };
    let Some(json) =
        frame.original_value("pin_resource_history", &[provider, cid, epoch], "ledger")
    else {
        return false;
    };
    let Some(envelope) = work
        .lock()
        .ok()
        .and_then(|mut work| work.typed_shared::<HistoryRoute>(json).ok())
    else {
        return true;
    };
    if envelope.ledger.provider != *provider || envelope.ledger.cid != *cid {
        return true;
    }
    envelope.ledger.route.is_some()
}

/// Only the exact persisted invocation contract proves an allocation domain.
/// Provider aliases and configuration names are never account evidence.
pub(crate) fn resource_route<'a>(
    frame: impl Into<crate::store::pinning::management::records::FrozenRows<'a>>,
    key: &NativeKey,
    work: &SharedDecodeBudget,
) -> Option<ProviderRouteSnapshot> {
    let frame = frame.into();
    let [provider, cid, rest @ ..] = key.values.as_slice() else {
        return None;
    };
    if key.table == "pin_resource_history" {
        let [epoch] = rest else {
            return None;
        };
        return history_route(frame, provider, cid, epoch, work);
    }
    if !matches!(key.table.as_str(), "remote_pins" | "remote_pin_ledger") || !rest.is_empty() {
        return None;
    }
    let original = decode_route(
        frame.original_value("remote_pin_ledger", &[provider, cid], "route")?,
        work,
    )?;
    if let Some(remote) = frame.row("remote_pins", &[provider, cid]) {
        let epoch = value(remote, "epoch")?;
        if frame
            .row("pin_resource_history", &[provider, cid, epoch])
            .is_some()
            && history_route(frame, provider, cid, epoch, work).as_ref() != Some(&original)
        {
            return None;
        }
    }
    Some(original)
}
