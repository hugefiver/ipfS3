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
    blocked_resources: BTreeSet<(String, String)>,
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
            scan.blocked_resources.clear();
            break;
        }
        remaining = remaining.saturating_sub(page.scanned);
        let scanned = page.scanned;
        let page_next = page.next;
        for candidate in page.candidates {
            let pair = (
                candidate.target.provider.clone(),
                candidate.target.cid.clone(),
            );
            if scan.blocked_resources.contains(&pair) {
                continue;
            }
            match advance_candidate(coordinator, store, now, &candidate).await {
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(target = %candidate.target.id, provider = %pair.0, cid = %pair.1, %error, "quota waiter deferred until the next scan")
                }
            }
            scan.blocked_resources.insert(pair);
        }
        scan.after = page_next;
        if scanned < page_limit || scan.after.is_none() {
            scan.after = None;
            scan.blocked_resources.clear();
            break;
        }
    }
    Ok(())
}

async fn advance_candidate(
    coordinator: &PinningCoordinator,
    store: &Store,
    now: DateTime<Utc>,
    candidate: &store_quota::QuotaWaiterCandidate,
) -> AppResult<bool> {
    let source = &candidate.target.provider;
    if candidate.target.logical_size < 0 {
        terminate_candidate(store, candidate, now).await?;
        return Ok(false);
    }
    let providers = match super::current_target_failover(
        store,
        coordinator,
        &candidate.lease,
        &candidate.target.cid,
        source,
        true,
    )
    .await
    {
        Ok(Some(providers)) => providers,
        Ok(None) | Err(AppError::InvalidPinningRequest(_)) => {
            terminate_candidate(store, candidate, now).await?;
            return Ok(false);
        }
        Err(error) => return Err(error),
    };
    if candidate.lease.provider_mode != "one" {
        return Ok(false);
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
            Ok(true)
        }
        Ok(None) => {
            tx.commit().await?;
            Ok(false)
        }
        Err(AppError::InvalidPinningRequest(_)) => {
            tx.rollback().await?;
            terminate_candidate(store, candidate, now).await?;
            Ok(false)
        }
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}
