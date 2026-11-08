use std::collections::BTreeSet;

use chrono::{DateTime, Utc};

use crate::{
    error::{AppError, AppResult},
    pinning::coordinator::PinningCoordinator,
    store::{
        Store,
        pinning::{
            leases,
            management::{
                admission,
                records::{DecodeWork, SharedDecodeBudget},
            },
            quota as store_quota,
        },
    },
};

const PAGE_SIZE: u64 = 4;

#[derive(Default)]
pub(super) struct ScanState {
    after: Option<store_quota::QuotaWaiterCursor>,
    blocked_sources: BTreeSet<String>,
}

#[cfg(test)]
static CANDIDATE_FAILURE: std::sync::LazyLock<tokio::sync::Mutex<Option<String>>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(None));

#[cfg(test)]
pub(super) async fn fail_candidate_once(target_id: &str) {
    *CANDIDATE_FAILURE.lock().await = Some(target_id.to_owned());
}

#[cfg(test)]
async fn stage_candidate_failure(target_id: &str) -> AppResult<()> {
    let mut failure = CANDIDATE_FAILURE.lock().await;
    if failure.as_deref() == Some(target_id) {
        failure.take();
        return Err(AppError::Database(
            "test quota waiter candidate failure".to_owned(),
        ));
    }
    Ok(())
}

pub(super) async fn terminate_candidate(
    store: &Store,
    candidate: &store_quota::QuotaWaiterCandidate,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let session = SharedDecodeBudget::default();
    let tx = store.begin_runtime(DecodeWork::shared(&session)).await?;
    match leases::terminate_quota_waiter(&tx, candidate, now).await {
        Ok(_) => tx.commit().await,
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}

#[cfg(test)]
pub(super) async fn coordinate(
    coordinator: &PinningCoordinator,
    store: &Store,
    now: DateTime<Utc>,
) -> AppResult<()> {
    coordinate_with_state(coordinator, store, now, &mut ScanState::default()).await
}

pub(super) async fn coordinate_with_state(
    coordinator: &PinningCoordinator,
    store: &Store,
    now: DateTime<Utc>,
    scan: &mut ScanState,
) -> AppResult<()> {
    let mut remaining = coordinator.settings().claim_limit;
    while remaining > 0 {
        let page_limit = remaining.min(PAGE_SIZE);
        let page = store_quota::quota_waiter_page(store.db(), scan.after.as_ref(), now, page_limit)
            .await?;
        if page.scanned == 0 {
            scan.after = None;
            scan.blocked_sources.clear();
            break;
        }
        remaining = remaining.saturating_sub(page.scanned);
        let scanned = page.scanned;
        let page_next = page.next;
        for candidate in page.candidates {
            let source = candidate.target.provider.clone();
            if scan.blocked_sources.contains(&source) {
                continue;
            }
            #[cfg(test)]
            stage_candidate_failure(&candidate.target.id).await?;
            if candidate.target.logical_size < 0 {
                terminate_candidate(store, &candidate, now).await?;
                scan.blocked_sources.insert(source);
                continue;
            }
            let providers = match super::current_target_failover(
                store,
                coordinator,
                &candidate.lease,
                &candidate.target.cid,
                &source,
                true,
            )
            .await
            {
                Ok(Some(providers)) => providers,
                Ok(None) | Err(AppError::InvalidPinningRequest(_)) => {
                    terminate_candidate(store, &candidate, now).await?;
                    scan.blocked_sources.insert(source);
                    continue;
                }
                Err(error) => return Err(error),
            };
            if candidate.lease.provider_mode != "one" {
                scan.blocked_sources.insert(source);
                continue;
            }
            let session = SharedDecodeBudget::default();
            let work = DecodeWork::shared(&session);
            let tx = store.begin_runtime(work).await?;
            let result = leases::fail_runtime_one_quota_waiter(
                &tx,
                &candidate.lease.id,
                &candidate.target.id,
                candidate.lease.generation,
                &providers,
                coordinator.provider_limits(),
                now,
                work,
            )
            .await
            .and_then(|replacement| {
                work.with(|session| session.check())
                    .map_err(admission::work_error)?;
                Ok(replacement)
            });
            match result {
                Ok(Some(replacement)) => {
                    tx.commit().await?;
                    tracing::debug!(
                        lease_id = %candidate.lease.id,
                        source_provider = %source,
                        destination_provider = %replacement.provider,
                        "pinning one-mode quota waiter advanced to current candidate"
                    );
                }
                Ok(None) => {
                    tx.commit().await?;
                    scan.blocked_sources.insert(source);
                }
                Err(AppError::InvalidPinningRequest(_)) => {
                    tx.rollback().await?;
                    terminate_candidate(store, &candidate, now).await?;
                    scan.blocked_sources.insert(source);
                }
                Err(error) => {
                    tx.rollback().await?;
                    return Err(error);
                }
            }
        }
        scan.after = page_next;
        if scanned < page_limit || scan.after.is_none() {
            scan.after = None;
            scan.blocked_sources.clear();
            break;
        }
    }
    Ok(())
}
