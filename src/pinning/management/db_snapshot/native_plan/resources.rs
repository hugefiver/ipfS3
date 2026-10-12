use super::*;
use crate::pinning::management::decision_scope::{DecisionScope, resource_route, resource_route_declared};
use crate::store::pinning::management::native::NativeKey;
use std::collections::BTreeMap;

struct Projection {
    remote: RemoteSnapshot,
    keys: Vec<NativeKey>,
}

fn keys(frame: &NativeFrame, evidence: &RemoteEvidence, history: bool) -> Vec<NativeKey> {
    frame.frontier.iter().filter(|key| {
        let correct_table = if history { key.table == "pin_resource_history" }
            else { matches!(key.table.as_str(), "remote_pins" | "remote_pin_ledger") };
        if !correct_table { return false; }
        let [provider, cid, rest @ ..] = key.values.as_slice() else { return false; };
        provider == &evidence.allocation && cid == &evidence.original_cid
            && if history { rest.len() == 1 && rest[0].parse::<i64>().ok() == evidence.epoch } else { rest.is_empty() }
    }).cloned().collect()
}

async fn one(tx: &DatabaseTransaction, frame: &NativeFrame, evidence: &RemoteEvidence, history: bool) -> Result<Option<Projection>> {
    let Some(epoch) = evidence.epoch.filter(|epoch| *epoch >= 0) else { return Ok(None); };
    if canonical(&evidence.original_cid).ok().as_ref() != Some(&evidence.cid) { return Ok(None); }
    let binding_key = NativeKey {
        table: if history { "pin_resource_history" } else { "remote_pin_ledger" }.into(),
        values: if history { vec![evidence.allocation.clone(), evidence.original_cid.clone(), epoch.to_string()] }
            else { vec![evidence.allocation.clone(), evidence.original_cid.clone()] },
    };
    let mut original = resource_route(frame, &binding_key);
    let raw_contract_exists = resource_route_declared(frame, &binding_key);
    let mut point_only = false;
    if original.is_none() && !raw_contract_exists {
        let point = crate::store::pinning::legacy_migration::resource_binding(tx, &evidence.allocation, &evidence.original_cid)
            .await.map_err(|_| SnapshotError::InvalidEvidence)?;
        original = point.filter(|point| !history || point.historical_epoch == Some(epoch)).and_then(|point| point.historical_route);
        point_only = original.is_some();
    }
    let Some(original) = original else { return Ok(None); };
    let route = route(&original);
    if super::super::super::validate::validate_route_identity(&route).is_err() { return Ok(None); }
    let remote = RemoteSnapshot {
        key: route.remote_key(&evidence.cid), route, resource_id: evidence.resource_id.clone(), epoch,
        effect: if point_only || (evidence.effect == SideEffect::Present && evidence.resource_id.is_none()) { SideEffect::Unknown } else { evidence.effect },
        ownership: if point_only { Ownership::Unknown } else { evidence.ownership },
        application_references: evidence.application_references, inflight_operations: evidence.inflight_jobs,
        retained: evidence.retained, cleanup_pending: evidence.cleanup_pending, accounted_bytes: evidence.accounted_bytes,
        confirmations: if point_only { vec![] } else { evidence.confirmations.clone() },
        confirmed_replicas: None, observation_current: None, replica_started_at: None,
    };
    Ok(Some(Projection { remote, keys: keys(frame, evidence, history) }))
}

fn decision_projection(rows: Vec<Projection>, history: bool, scopes: &[DecisionScope], full: bool, colliding_epochs: &BTreeSet<(RemoteKey,i64)>, represented: &mut BTreeSet<NativeKey>) -> Vec<RemoteSnapshot> {
    let mut multiplicity = BTreeMap::new();
    for row in &rows {
        *multiplicity.entry((row.remote.key.clone(), history.then_some(row.remote.epoch))).or_insert(0usize) += 1;
    }
    rows.into_iter().filter_map(|row| {
        // A canonical collision has no preferred alias. Keep every original PK
        // unknown, rather than deduplicating one alias into execution authority.
        if multiplicity[&(row.remote.key.clone(), history.then_some(row.remote.epoch))] != 1
            || colliding_epochs.contains(&(row.remote.key.clone(),row.remote.epoch))
            || (!full && !scopes.iter().any(|scope| scope.includes(&row.remote.key)))
        { return None; }
        represented.extend(row.keys);
        Some(row.remote)
    }).collect()
}

pub(super) async fn project(tx: &DatabaseTransaction, frame: &NativeFrame, diagnostic: &DatabaseSnapshot, request: &PlanRequest, snapshot: &Snapshot) -> Result<(Vec<RemoteSnapshot>, Vec<RemoteSnapshot>, Vec<NativeKey>)> {
    let scopes = request.items.iter().map(|item| DecisionScope::for_item(item, snapshot)).collect::<Vec<_>>();
    let full = request.items.iter().any(|item| matches!(item.subject, Subject::Provider { .. }));
    let mut current = Vec::new(); let mut historical = Vec::new();
    for (rows, history) in [(&diagnostic.remotes, false), (&diagnostic.historical_remotes, true)] {
        for evidence in rows {
            if let Some(row) = one(tx, frame, evidence, history).await? {
                if history { historical.push(row); } else { current.push(row); }
            }
        }
    }
    // A confirmed release preserves the current remote epoch and archives that
    // same epoch. Both collections participate before any original PK is marked
    // represented; neither current nor history is a preferred execution alias.
    let mut epochs=BTreeMap::new();
    for row in current.iter().chain(&historical) {
        *epochs.entry((row.remote.key.clone(),row.remote.epoch)).or_insert(0usize)+=1;
    }
    let colliding_epochs=epochs.into_iter().filter_map(|(key,count)|(count>1).then_some(key)).collect::<BTreeSet<_>>();
    let mut represented = BTreeSet::new();
    let current = decision_projection(current, false, &scopes, full, &colliding_epochs, &mut represented);
    let historical = decision_projection(historical, true, &scopes, full, &colliding_epochs, &mut represented);
    let unknown = frame.frontier.iter().filter(|key| matches!(key.table.as_str(), "remote_pins" | "remote_pin_ledger" | "pin_resource_history")
        && !represented.contains(*key)).cloned().collect();
    Ok((current, historical, unknown))
}
