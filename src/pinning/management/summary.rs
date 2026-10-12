use std::collections::{BTreeMap, BTreeSet};

use super::model::*;

fn current_or_captured<'a>(
    snapshot: &'a Snapshot,
    captured: &'a RemoteSnapshot,
) -> &'a RemoteSnapshot {
    // Missing a ledger row is not proof of release: preserve captured occupancy.
    snapshot
        .remotes
        .iter()
        .chain(&snapshot.historical_remotes)
        .find(|remote| remote.key == captured.key && remote.epoch == captured.epoch)
        .or_else(|| {
            if matches!(
                captured.effect,
                SideEffect::NotSubmitted | SideEffect::Rejected
            ) {
                snapshot
                    .remotes
                    .iter()
                    .find(|remote| remote.key == captured.key)
            } else {
                None
            }
        })
        .unwrap_or(captured)
}

#[derive(Default)]
struct ResourceQuote {
    bytes: u64,
    unknown: bool,
    inflight: bool,
    cleanup_pending: bool,
}

type QuoteKey = (RemoteKey, Option<String>, i64);

fn add_remote(resources: &mut BTreeMap<QuoteKey, ResourceQuote>, remote: &RemoteSnapshot) {
    // Known opaque identity deduplicates observations across epochs; unresolved
    // attempts without an id retain their separate epoch reservations.
    let epoch = if remote.resource_id.is_some() {
        0
    } else {
        remote.epoch
    };
    let quote = resources
        .entry((remote.key.clone(), remote.resource_id.clone(), epoch))
        .or_default();
    if let Some(bytes) = remote.accounted_bytes { quote.bytes = quote.bytes.max(bytes); }
    quote.unknown |= remote.has_unknown_responsibility();
    quote.inflight |= remote.effect == SideEffect::InFlight || remote.inflight_operations.is_some_and(|n|n > 0);
    quote.cleanup_pending |= remote.cleanup_pending;
}

pub(crate) fn summarize(entries: &[PlanEntry], snapshot: &Snapshot) -> Result<Summary, SafeError> {
    let mut summary = Summary {
        items: entries.len(),
        unknown_resources: snapshot.unknown_resources.len(),
        ..Summary::default()
    };
    let mut cids = BTreeSet::new();
    let mut resources = BTreeMap::new();
    let mut transfers = BTreeSet::new();
    for entry in entries {
        let applied = snapshot
            .applied
            .iter()
            .any(|receipt| receipt.binding == *entry);
        if applied {
            summary.already_applied += 1;
        }
        match entry.status {
            PlanStatus::Candidate => summary.candidates += 1,
            PlanStatus::Blocked(_) => summary.blocked += 1,
            PlanStatus::Ready => {}
        }
        match &entry.capture {
            Some(Capture::Object(capture)) => {
                summary.logical_targets += 1;
                cids.insert(&capture.object.identity.cid);
                let logical_bytes = capture
                    .target
                    .as_ref()
                    .map_or(capture.object.logical_bytes, |target| target.logical_bytes);
                summary.logical_intent_bytes = summary
                    .logical_intent_bytes
                    .checked_add(logical_bytes)
                    .ok_or(SafeError::LimitExceeded)?;
                if capture.destination_route.is_some() {
                    summary.logical_targets += 1;
                    summary.logical_intent_bytes = summary
                        .logical_intent_bytes
                        .checked_add(logical_bytes)
                        .ok_or(SafeError::LimitExceeded)?;
                }
                for remote in [&capture.remote, &capture.destination_remote]
                    .into_iter()
                    .flatten()
                    .chain(&capture.historical_remotes)
                {
                    let remote = current_or_captured(snapshot, remote);
                    add_remote(&mut resources, remote);
                }
                // Newly recorded effects after application still appear in the quote.
                for route in [Some(&capture.route), capture.destination_route.as_ref()]
                    .into_iter()
                    .flatten()
                {
                    let key = route.identity.remote_key(&capture.object.identity.cid);
                    for remote in snapshot
                        .remotes
                        .iter()
                        .chain(&snapshot.historical_remotes)
                        .filter(|remote| remote.key == key)
                    {
                        add_remote(&mut resources, remote);
                    }
                }
                if !applied && let Some(PlannedStep::Object(step)) = &entry.step {
                    if step.source == RemoteDirective::SubmitWithFence {
                        transfers.insert(
                            capture
                                .route
                                .identity
                                .remote_key(&capture.object.identity.cid),
                        );
                    }
                    if step.destination == Some(RemoteDirective::SubmitWithFence)
                        && let Some(route) = &capture.destination_route
                    {
                        transfers.insert(route.identity.remote_key(&capture.object.identity.cid));
                    }
                }
            }
            Some(Capture::Provider(capture)) => {
                summary.logical_targets += capture.targets.len();
                for target in &capture.targets {
                    cids.insert(&target.cid);
                    summary.logical_intent_bytes = summary
                        .logical_intent_bytes
                        .checked_add(target.logical_bytes)
                        .ok_or(SafeError::LimitExceeded)?;
                }
                for remote in capture.remotes.iter().chain(&capture.historical_remotes) {
                    let remote = current_or_captured(snapshot, remote);
                    add_remote(&mut resources, remote);
                    cids.insert(&remote.key.cid);
                }
            }
            None => {}
        }
    }
    summary.unique_cids = cids.len();
    summary.unique_resources = resources.len();
    summary.possible_new_transfers = transfers.len();
    for remote in resources.values() {
        summary.accounted_remote_bytes = summary
            .accounted_remote_bytes
            .checked_add(remote.bytes)
            .ok_or(SafeError::LimitExceeded)?;
        summary.unknown_resources += usize::from(remote.unknown);
        summary.inflight_resources += usize::from(remote.inflight);
        summary.cleanup_pending_resources += usize::from(remote.cleanup_pending);
    }
    Ok(summary)
}
