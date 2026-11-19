use std::{collections::BTreeMap, sync::Arc, time::Duration};

use chrono::{TimeDelta, Utc};
use tokio::{task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    pinning::{
        coordinator::PinningCoordinator,
        provider::{
            PinningProvider, ProviderError, ProviderErrorClass, RemotePinStatus, SubmitPin,
        },
    },
    store::{
        Store,
        pinning::resources::{self, Action, Claim, Outcome},
    },
};

#[path = "worker_dispatch.rs"]
mod dispatch;

#[cfg(test)]
#[path = "worker_tests.rs"]
mod tests;

pub struct PinningWorkerHandle {
    cancellation: CancellationToken,
    join: JoinHandle<()>,
}

impl PinningWorkerHandle {
    pub async fn shutdown(self, grace: Duration) {
        self.cancellation.cancel();
        let mut join = self.join;
        if tokio::time::timeout(grace, &mut join).await.is_err() {
            join.abort();
            let _ = join.await;
        }
    }
}

pub(crate) fn start(
    coordinator: Arc<PinningCoordinator>,
    store: Store,
    parent: CancellationToken,
) -> PinningWorkerHandle {
    let cancellation = parent.child_token();
    let stop = cancellation.clone();
    let join = tokio::spawn(async move {
        let config = coordinator.effective_config();
        let interval = Duration::from_secs(config.worker_interval.as_seconds());
        let lanes = coordinator
            .provider_limits()
            .keys()
            .map(|name| {
                dispatch::Lane::new(
                    coordinator.provider(name).unwrap(),
                    coordinator.request_interval(name),
                )
            })
            .collect();
        tokio::join!(
            dispatch::run(
                store.clone(),
                lanes,
                coordinator.provider_limits(),
                config.worker_concurrency,
                interval,
                coordinator.subscribe_pin_work(),
                stop.clone()
            ),
            expire_references(store, interval, stop, &coordinator),
        );
    });
    PinningWorkerHandle { cancellation, join }
}

async fn expire_references(
    store: Store,
    interval: Duration,
    stop: CancellationToken,
    coordinator: &PinningCoordinator,
) {
    const PAGE: u32 = 128;
    loop {
        let result = tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            result = resources::expire(store.db(), Utc::now(), PAGE) => result,
        };
        match result {
            Ok(count) if count > 0 => {
                coordinator.notify_pin_work();
                if count >= u64::from(PAGE) {
                    continue;
                }
            }
            Err(error) => tracing::warn!(%error, "could not expire pin references"),
            _ => {}
        }
        tokio::select! { _ = stop.cancelled() => return, _ = tokio::time::sleep(interval) => {} }
    }
}

enum Rescan {
    None,
    Cleanup,
    Provider,
    All,
}

async fn perform(store: Store, provider: Arc<dyn PinningProvider>, claim: Claim) -> Rescan {
    let log_timing = tracing::enabled!(tracing::Level::DEBUG);
    tracing::debug!(name: "pin_dispatched", provider=%claim.provider, cid=%claim.cid, source_cid=%claim.source_cid, action=?claim.action, attempt=claim.attempt, "pin action dispatched");
    let provider_started = log_timing.then(Instant::now);
    let outcome = execute(provider.as_ref(), &claim).await;
    let elapsed_us = provider_started.map(|start| start.elapsed().as_micros() as u64);
    tracing::debug!(name: "pin_provider_complete", provider=%claim.provider, cid=%claim.cid, action=?claim.action, attempt=claim.attempt, elapsed_us, "pin provider action finished");
    let settle_started = log_timing.then(Instant::now);
    let mut cleanup = None;
    for attempt in 0..3 {
        match resources::settle_dispatch(store.db(), &claim, outcome.clone(), Utc::now()).await {
            Ok(due_delete) => {
                cleanup = Some(due_delete);
                break;
            }
            Err(crate::error::AppError::Database(_)) if attempt < 2 => {
                tokio::time::sleep(Duration::from_millis(250 << attempt)).await;
            }
            Err(error) => {
                tracing::error!(%error, provider=%claim.provider, cid=%claim.cid, "could not save pin action result");
                break;
            }
        }
    }
    let elapsed_us = settle_started.map(|start| start.elapsed().as_micros() as u64);
    tracing::debug!(name: "pin_settle_complete", provider=%claim.provider, cid=%claim.cid, action=?claim.action, attempt=claim.attempt, elapsed_us, settled=cleanup.is_some(), "pin writeback finished");
    match (cleanup, claim.action, outcome) {
        (Some(true), _, _) => Rescan::Cleanup,
        (Some(false), Action::Submit, Outcome::Failed) => Rescan::All,
        (Some(false), _, Outcome::Deleted) | (Some(false), Action::Submit, Outcome::RetryAt(_)) => {
            Rescan::Provider
        }
        _ => Rescan::None,
    }
}

async fn execute(provider: &dyn PinningProvider, claim: &Claim) -> Outcome {
    let error = match claim.action {
        Action::Submit => {
            let request = SubmitPin {
                cid: claim.source_cid.clone(),
                name: claim.cid.clone(),
                metadata: BTreeMap::from([("gateway_job_id".into(), claim.token.clone())]),
            };
            match provider.submit(request).await {
                Ok(pin) if pin.status == RemotePinStatus::Failed => {
                    return Outcome::Rejected(pin.request_id);
                }
                Ok(pin) => return Outcome::Submitted(pin.request_id),
                Err(error) if !error.definitely_not_submitted() => {
                    tracing::warn!(provider=%claim.provider, cid=%claim.cid, class=?error.class, "pin submission has unknown effect");
                    return Outcome::Unknown;
                }
                Err(error) => error,
            }
        }
        Action::Delete => match provider
            .unpin(claim.handle.as_deref().expect("delete claim has a handle"))
            .await
        {
            Ok(()) => return Outcome::Deleted,
            Err(error) => error,
        },
    };
    tracing::warn!(provider=%claim.provider, cid=%claim.cid, action=?claim.action, class=?error.class, "pin action failed");
    retry_or_fail(error, claim.attempt)
}

fn retry_or_fail(error: ProviderError, attempt: i32) -> Outcome {
    use ProviderErrorClass::{Ambiguous, NotSubmitted, Quota, RateLimited, Transient};
    if attempt >= 8
        || !matches!(
            error.class,
            RateLimited | Quota | NotSubmitted | Transient | Ambiguous
        )
    {
        return Outcome::Failed;
    }
    let delay = error
        .retry_after
        .unwrap_or(Duration::from_secs(1 << (attempt - 1)))
        .min(Duration::from_secs(300));
    Outcome::RetryAt(Utc::now() + TimeDelta::from_std(delay).unwrap())
}
