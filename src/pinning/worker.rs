use std::{collections::BTreeMap, sync::Arc, time::Duration};

use chrono::{TimeDelta, Utc};
use tokio::{
    sync::Semaphore,
    task::{JoinHandle, JoinSet},
    time::{Instant, sleep_until},
};
use tokio_util::sync::CancellationToken;

use crate::{
    pinning::{
        config::ProviderLimits,
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
        let slots = Arc::new(Semaphore::new(config.worker_concurrency));
        let interval = Duration::from_secs(config.worker_interval.as_seconds());
        let mut tasks = JoinSet::new();
        for (name, limits) in coordinator.provider_limits() {
            tasks.spawn(run_provider(
                store.clone(),
                coordinator.provider(name).unwrap(),
                limits.clone(),
                slots.clone(),
                interval,
                coordinator.request_interval(name),
                stop.clone(),
            ));
        }
        tasks.spawn(expire_references(store, interval, stop));
        while let Some(result) = tasks.join_next().await {
            if let Err(error) = result {
                tracing::error!(%error, "pin worker stopped unexpectedly");
            }
        }
    });
    PinningWorkerHandle { cancellation, join }
}

async fn expire_references(store: Store, interval: Duration, stop: CancellationToken) {
    const PAGE: u32 = 128;
    loop {
        let result = tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            result = resources::expire(store.db(), Utc::now(), PAGE) => result,
        };
        match result {
            Ok(count) if count >= u64::from(PAGE) => continue,
            Err(error) => tracing::warn!(%error, "could not expire pin references"),
            _ => {}
        }
        tokio::select! { _ = stop.cancelled() => return, _ = tokio::time::sleep(interval) => {} }
    }
}

async fn run_provider(
    store: Store,
    provider: Arc<dyn PinningProvider>,
    limits: ProviderLimits,
    slots: Arc<Semaphore>,
    idle: Duration,
    min_interval: Duration,
    stop: CancellationToken,
) {
    let mut requests = JoinSet::new();
    let mut next = Instant::now();
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            Some(result) = requests.join_next(), if !requests.is_empty() => {
                if let Err(error) = result { tracing::error!(%error, "pin request task stopped unexpectedly"); }
                continue;
            }
            _ = sleep_until(next) => {}
        };
        if slots.available_permits() == 0 {
            tokio::select! {
                _ = stop.cancelled() => break,
                permit = slots.acquire() => drop(permit.unwrap()),
            }
        }
        let now = Utc::now();
        match resources::claim(
            store.db(),
            provider.name(),
            &limits,
            now,
            now + TimeDelta::minutes(5),
        )
        .await
        {
            Ok(Some(claim)) => {
                let slot = tokio::select! {
                    biased;
                    _ = stop.cancelled() => {
                        if let Err(error) = resources::settle(store.db(), &claim, Outcome::RetryAt(Utc::now()), Utc::now()).await {
                            tracing::error!(%error, "could not return undispatched pin action");
                        }
                        break;
                    }
                    permit = slots.clone().acquire_owned() => permit.unwrap(),
                };
                let store = store.clone();
                let provider = provider.clone();
                requests.spawn(async move {
                    let outcome = execute(provider.as_ref(), &claim).await;
                    drop(slot);
                    for attempt in 0..3 {
                        match resources::settle(store.db(), &claim, outcome.clone(), Utc::now()).await {
                            Ok(()) => break,
                            Err(crate::error::AppError::Database(_)) if attempt < 2 => {
                                tokio::time::sleep(Duration::from_millis(250 << attempt)).await;
                            }
                            Err(error) => {
                                tracing::error!(%error, provider=%claim.provider, cid=%claim.cid, "could not save pin action result");
                                break;
                            }
                        }
                    }
                });
                next = Instant::now() + min_interval;
            }
            result => {
                if let Err(error) = result {
                    tracing::warn!(%error, provider=provider.name(), "could not claim pin action");
                }
                next = Instant::now() + idle;
            }
        }
    }
    while requests.join_next().await.is_some() {}
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
