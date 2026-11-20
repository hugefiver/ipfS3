use std::collections::{BTreeMap, VecDeque};

use tokio::{sync::watch, task::JoinSet, time::sleep_until};

use super::*;
use crate::pinning::config::{ProviderLimitMap, ProviderLimits};
use resources::{BatchClaims, Candidate, MAX_CANDIDATES};

struct Offer {
    rows: Vec<Candidate>,
    lanes: Vec<usize>,
    budgets: BTreeMap<String, usize>,
}

impl Offer {
    fn take(lanes: &mut [Lane], ready: &[usize], free: usize, cursor: &mut usize) -> Self {
        let mut rows = Vec::new();
        let mut owners = Vec::new();
        let mut budgets = BTreeMap::new();
        for round in 0..free {
            let before = rows.len();
            for &i in ready {
                let lane = &mut lanes[i];
                if (round == 0 || lane.interval.is_zero())
                    && let Some(row) = lane.page.pop_front()
                {
                    *budgets.entry(row.provider.clone()).or_default() += 1;
                    rows.push(row);
                    owners.push(i);
                    if rows.len() == free {
                        break;
                    }
                }
            }
            if before == rows.len() || rows.len() == free {
                break;
            }
        }
        *cursor = (owners.last().unwrap() + 1) % lanes.len();
        // If the last prefix held only stale or oversized rows, find a fitting
        // candidate in one reservation transaction without raising its budget.
        for &i in ready {
            if lanes[i].look_ahead {
                let count = lanes[i].page.len().min(MAX_CANDIDATES - rows.len());
                rows.extend(lanes[i].page.drain(..count));
                owners.extend(std::iter::repeat_n(i, count));
            }
        }
        Self {
            rows,
            lanes: owners,
            budgets,
        }
    }
}

pub(super) async fn run(
    store: Store,
    mut lanes: Vec<Lane>,
    limits: &ProviderLimitMap,
    width: usize,
    idle: Duration,
    mut wake: watch::Receiver<()>,
    stop: CancellationToken,
) {
    if lanes.is_empty() {
        return;
    }
    let indices: BTreeMap<_, _> = lanes
        .iter()
        .enumerate()
        .map(|(i, lane)| (lane.provider.name().to_owned(), i))
        .collect();
    let mut tasks = JoinSet::<(usize, Rescan)>::new();
    let mut cursor = 0;
    loop {
        let deadline = lanes.iter().map(Lane::ready_at).min().unwrap();
        let first = tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            Some(result) = tasks.join_next(), if !tasks.is_empty() => Some(result),
            Ok(()) = wake.changed() => {
                lanes.iter_mut().for_each(Lane::wake);
                None
            }
            _ = sleep_until(deadline), if tasks.len() < width => None,
        };
        for result in first
            .into_iter()
            .chain(std::iter::from_fn(|| tasks.try_join_next()))
        {
            match result {
                Ok((i, Rescan::All)) => {
                    lanes[i].submit_blocked_until = None;
                    lanes.iter_mut().for_each(Lane::wake);
                }
                Ok((i, Rescan::Provider)) => {
                    lanes[i].submit_blocked_until = None;
                    lanes[i].wake();
                }
                Ok((i, Rescan::Cleanup)) => lanes[i].wake(),
                Ok((_, Rescan::None)) => {}
                Err(error) => tracing::error!(%error, "pin request task stopped unexpectedly"),
            }
        }
        let free = (width - tasks.len()).min(MAX_CANDIDATES);
        if free == 0 {
            continue;
        }
        let mut ready = Vec::new();
        for offset in 0..lanes.len() {
            let i = (cursor + offset) % lanes.len();
            let lane = &mut lanes[i];
            if lane.ready_at() > Instant::now() {
                continue;
            }
            if lane.page.is_empty() {
                lane.fill(&store, &limits[lane.provider.name()], idle).await;
            }
            if !lane.page.is_empty() {
                ready.push(i);
                if ready.len() == free {
                    break;
                }
            }
        }
        if ready.is_empty() || stop.is_cancelled() {
            continue;
        }
        let offer = Offer::take(&mut lanes, &ready, free, &mut cursor);
        let now = Utc::now();
        let BatchClaims {
            claims,
            consumed,
            blocked,
        } = match resources::claim_batch(
            store.db(),
            &offer.rows,
            limits,
            &offer.budgets,
            free,
            now,
            now + TimeDelta::minutes(5),
        )
        .await
        {
            Ok(batch) => batch,
            Err(error) => {
                tracing::warn!(%error, "could not claim pin actions");
                for (row, i) in offer.rows.into_iter().zip(offer.lanes).rev() {
                    lanes[i].page.push_front(row);
                    lanes[i].eligible_at = Instant::now() + idle;
                }
                continue;
            }
        };
        for i in ready {
            lanes[i].look_ahead = true;
        }
        for claim in claims {
            let i = indices[&claim.provider];
            let lane = &mut lanes[i];
            lane.look_ahead = false;
            if let Some(metrics) = &lane.metrics {
                // Legacy immediate schedules used zero instead of a timestamp.
                let wait = (claim.eligible_at_micros > 0).then(|| {
                    Duration::from_micros(
                        (Utc::now().timestamp_micros() - claim.eligible_at_micros).max(0) as u64,
                    )
                });
                metrics.dispatch(claim.action, wait);
            }
            let request = perform(
                store.clone(),
                lane.provider.clone(),
                claim,
                lane.metrics.clone(),
            );
            tasks.spawn(async move { (i, request.await) });
            lane.eligible_at = Instant::now() + lane.interval;
        }
        let mut used_rows = vec![false; offer.rows.len()];
        for index in consumed {
            used_rows[index] = true;
        }
        for ((row, i), used) in offer.rows.into_iter().zip(offer.lanes).zip(used_rows).rev() {
            if !used {
                lanes[i].page.push_front(row);
            }
        }
        for name in blocked {
            let lane = &mut lanes[indices[&name]];
            lane.page.clear();
            lane.after = None;
            lane.revisit_head = false;
            lane.scan_at = Instant::now() + idle;
            lane.submit_blocked_until = Some(lane.scan_at);
            tracing::trace!(name: "pin_worker_idle", provider=%name, "pin reservation unavailable");
        }
    }
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            tracing::error!(%error, "pin request task stopped unexpectedly");
        }
    }
}

pub(super) struct Lane {
    provider: Arc<dyn PinningProvider>,
    metrics: Option<Arc<PinMetrics>>,
    interval: Duration,
    eligible_at: Instant,
    scan_at: Instant,
    page: VecDeque<Candidate>,
    after: Option<Candidate>,
    look_ahead: bool,
    revisit_head: bool,
    submit_blocked_until: Option<Instant>,
}

impl Lane {
    pub(super) fn new(
        provider: Arc<dyn PinningProvider>,
        interval: Duration,
        metrics: Option<Arc<PinMetrics>>,
    ) -> Self {
        let now = Instant::now();
        Self {
            provider,
            metrics,
            interval,
            eligible_at: now,
            scan_at: now,
            page: VecDeque::new(),
            after: None,
            look_ahead: false,
            revisit_head: false,
            submit_blocked_until: None,
        }
    }

    fn wake(&mut self) {
        self.scan_at = Instant::now();
        // Finish the keyset pass even if new publications keep waking this lane.
        self.revisit_head |= self.after.is_some();
    }

    fn ready_at(&self) -> Instant {
        if self.page.is_empty() {
            self.scan_at.max(self.eligible_at)
        } else {
            self.eligible_at
        }
    }

    async fn fill(&mut self, store: &Store, limits: &ProviderLimits, idle: Duration) {
        let name = self.provider.name();
        let now = Instant::now();
        self.submit_blocked_until = self.submit_blocked_until.filter(|until| *until > now);
        // A publish hint can reveal a delete while quota is still full. Avoid
        // repeating submit reservations until cleanup settles or the idle tick.
        let mut scan_limits = limits.clone();
        scan_limits.enabled &= self.submit_blocked_until.is_none();
        match resources::candidate_page(
            store.db(),
            name,
            &scan_limits,
            self.after.as_ref(),
            MAX_CANDIDATES,
            Utc::now(),
        )
        .await
        {
            Ok(rows) => {
                tracing::trace!(name: "pin_candidate_page", provider=name, count=rows.len(), "read pin candidates");
                if rows.len() == MAX_CANDIDATES {
                    self.after = rows.last().cloned();
                } else {
                    self.after = None;
                    self.scan_at = if std::mem::take(&mut self.revisit_head) {
                        Instant::now()
                    } else {
                        self.submit_blocked_until
                            .unwrap_or_else(|| Instant::now() + idle)
                    };
                }
                if rows.is_empty() {
                    tracing::trace!(name: "pin_worker_idle", provider=name, "pin scan found no due action");
                }
                self.page = rows.into();
            }
            Err(error) => {
                tracing::warn!(%error, provider=name, "could not read pin candidates");
                self.eligible_at = Instant::now() + idle;
            }
        }
    }
}
