use std::collections::{BTreeMap, BTreeSet};

use super::model::*;

fn current_or_captured<'a>(
    snapshot: &'a super::SnapshotView<'_>,
    captured: &'a RemoteSnapshot,
) -> &'a RemoteSnapshot {
    // Missing a ledger row is not proof of release: preserve captured occupancy.
    snapshot
        .remotes
        .iter()
        .chain(snapshot.historical_remotes)
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
    if let Some(bytes) = remote.accounted_bytes {
        quote.bytes = quote.bytes.max(bytes);
    }
    quote.unknown |= remote.has_unknown_responsibility();
    quote.inflight |=
        remote.effect == SideEffect::InFlight || remote.inflight_operations.is_some_and(|n| n > 0);
    quote.cleanup_pending |= remote.cleanup_pending;
}

pub(crate) fn summarize(
    entries: &[PlanEntry],
    snapshot: &super::SnapshotView<'_>,
) -> Result<Summary, SafeError> {
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
                let cid = capture
                    .target
                    .as_ref()
                    .map_or(capture.object.identity.cid.as_str(), |target| {
                        target.cid.as_str()
                    });
                cids.insert(cid);
                let routes = capture
                    .requirement_routes
                    .iter()
                    .chain(std::iter::once(&capture.route))
                    .chain(capture.destination_route.iter())
                    .map(|route| (route.identity.remote_key(cid), route))
                    .collect::<BTreeMap<_, _>>();
                summary.logical_targets += routes.len();
                let logical_bytes = capture
                    .target
                    .as_ref()
                    .map_or(capture.object.logical_bytes, |target| target.logical_bytes);
                summary.logical_intent_bytes = summary
                    .logical_intent_bytes
                    .checked_add(
                        logical_bytes
                            .checked_mul(
                                u64::try_from(routes.len())
                                    .map_err(|_| SafeError::LimitExceeded)?,
                            )
                            .ok_or(SafeError::LimitExceeded)?,
                    )
                    .ok_or(SafeError::LimitExceeded)?;
                for remote in [&capture.remote, &capture.destination_remote]
                    .into_iter()
                    .flatten()
                    .chain(&capture.historical_remotes)
                {
                    let remote = current_or_captured(snapshot, remote);
                    add_remote(&mut resources, remote);
                }
                // Newly recorded effects after application still appear in the quote.
                for key in routes.keys() {
                    for remote in snapshot
                        .remotes
                        .iter()
                        .chain(snapshot.historical_remotes)
                        .filter(|remote| &remote.key == key)
                    {
                        add_remote(&mut resources, remote);
                    }
                }
                if !applied && let Some(PlannedStep::Object(step)) = &entry.step {
                    if let Action::Create(intent)
                    | Action::Reapply(intent)
                    | Action::Backfill(intent) = &entry.request.action
                    {
                        let now = snapshot.database_now.unwrap_or_else(|| {
                            chrono::DateTime::from_timestamp(snapshot.observed_at, 0)
                                .expect("validated database clock")
                        });
                        for route in &capture.requirement_routes {
                            if !route.available
                                || !route.allocation_allowed
                                || !capture.object.allowed_routes.contains(&route.identity)
                            {
                                continue;
                            }
                            let key = route.identity.remote_key(cid);
                            let remote = snapshot.remotes.iter().find(|remote| remote.key == key);
                            let historical = remote.and_then(|remote| {
                                snapshot
                                    .routes
                                    .iter()
                                    .find(|route| route.identity == remote.route)
                            });
                            if super::semantics::directive(
                                capture,
                                route,
                                remote,
                                historical,
                                &intent.requirements,
                                now,
                            ) == Ok(RemoteDirective::SubmitWithFence)
                            {
                                transfers.insert(key);
                            }
                        }
                    }
                    if step.source == RemoteDirective::SubmitWithFence {
                        transfers.insert(capture.route.identity.remote_key(cid));
                    }
                    if step.destination == Some(RemoteDirective::SubmitWithFence)
                        && let Some(route) = &capture.destination_route
                    {
                        transfers.insert(route.identity.remote_key(cid));
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
    summary.unique_resources = resources.len()
        + transfers
            .iter()
            .filter(|key| !resources.keys().any(|(captured, _, _)| captured == *key))
            .count();
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
