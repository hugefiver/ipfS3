//! Root-only recovery: no ZIP bytes, S3 object publication or remote pin jobs.

use std::time::Duration;

use sea_orm::DatabaseConnection;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    error::AppResult,
    kubo::{
        KuboClient,
        directory::{DirectoryBuildError, build_directory},
    },
    store::{
        Store,
        zip::{self, recovery as durable},
    },
};

const LEASE_SECONDS: i64 = 600; // builder deadline is five minutes, with room for DB settlement
const SCAN_INTERVAL: Duration = Duration::from_secs(30);
const MAX_IN_FLIGHT: usize = 2;

pub struct RecoveryWorker {
    cancel: CancellationToken,
    join: JoinHandle<()>,
}

impl RecoveryWorker {
    pub async fn shutdown(self, grace: Duration) {
        self.cancel.cancel();
        let mut join = self.join;
        if tokio::time::timeout(grace, &mut join).await.is_err() {
            join.abort();
            let _ = join.await;
        }
    }

    #[doc(hidden)]
    pub fn abort_for_test(self) -> JoinHandle<()> {
        self.join.abort();
        self.join
    }
}

pub fn start_worker(store: Store, kubo: KuboClient, parent: CancellationToken) -> RecoveryWorker {
    let cancel = parent.child_token();
    let worker_cancel = cancel.clone();
    let join = tokio::spawn(async move {
        let mut interval = tokio::time::interval(SCAN_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut roots = JoinSet::new();
        let mut pending = Vec::new().into_iter();
        loop {
            tokio::select! {
                biased;
                _ = worker_cancel.cancelled() => break,
                Some(joined) = roots.join_next(), if !roots.is_empty() => match joined {
                    Ok(false) if pending.len() == 0 => continue,
                    Err(error) => tracing::warn!(%error, "ZIP root recovery task failed"),
                    _ => {}
                },
                _ = interval.tick(), if roots.len() < MAX_IN_FLIGHT => {}
            }
            if pending.len() == 0 {
                match durable::due_page(store.db()).await {
                    Ok(ids) => pending = ids.into_iter(),
                    Err(error) => {
                        tracing::warn!(%error, "ZIP root recovery scan failed");
                        continue;
                    }
                }
            }
            for id in pending.by_ref().take(MAX_IN_FLIGHT - roots.len()) {
                let db = store.db().clone();
                let kubo = kubo.clone();
                let cancel = worker_cancel.clone();
                roots.spawn(async move { recover_one(&db, &kubo, &cancel, id).await });
            }
        }
        // Let builders retain known candidates under their original claims.
        while roots.join_next().await.is_some() {}
    });
    RecoveryWorker { cancel, join }
}

/// One bounded page, one root at a time. Ownership races are normal: only a
/// successful DB-fenced claim may begin any Kubo RPC.
pub async fn run_page(
    db: &DatabaseConnection,
    kubo: &KuboClient,
    cancel: &CancellationToken,
) -> AppResult<usize> {
    let mut attempted = 0;
    for id in durable::due_page(db).await? {
        if cancel.is_cancelled() {
            break;
        }
        attempted += usize::from(recover_one(db, kubo, cancel, id).await);
    }
    Ok(attempted)
}

async fn recover_one(
    db: &DatabaseConnection,
    kubo: &KuboClient,
    cancel: &CancellationToken,
    id: String,
) -> bool {
    if cancel.is_cancelled() {
        return false;
    }
    let worker = uuid::Uuid::new_v4().to_string();
    let Ok(claim) = zip::claim_root(db, &id, &worker, LEASE_SECONDS).await else {
        return false;
    };
    if let Err(error) = recover_claimed(db, kubo, cancel, &claim).await {
        tracing::warn!(%error, batch_id = %id, "ZIP root recovery claim interrupted; lease will expire");
    }
    true
}

async fn recover_claimed(
    db: &DatabaseConnection,
    kubo: &KuboClient,
    cancel: &CancellationToken,
    claim: &zip::RootClaim,
) -> AppResult<()> {
    let snapshot = zip::snapshot(db, &claim.batch_id).await?.ok_or_else(|| {
        crate::error::AppError::Internal("ZIP root recovery batch disappeared".into())
    })?;
    // Revision five may have crashed before recording its terminal outcome.
    // Fence that attempt, then stop without another root RPC.
    if claim.revision > durable::MAX_REVISIONS {
        durable::settle_failed(db, &snapshot, claim, "needs_attention").await?;
        return Ok(());
    }
    let files = match durable::files(&snapshot, claim) {
        Ok(files) => files,
        Err(_) => {
            durable::settle_failed(db, &snapshot, claim, "invalid_manifest").await?;
            return Ok(());
        }
    };
    if cancel.is_cancelled() {
        return Ok(());
    }
    zip::mark_invoked(db, claim).await?;
    let built = build_directory(kubo, &files, cancel).await;
    // Cancellation must not discard a known CID; retain it under this claim
    // before stopping, without verifying or adopting it.
    match built {
        Ok(Some(root)) => {
            let node = root.local_residency.node_identity;
            let cid = root.cid;
            zip::retain_candidate(db, claim, &node, "hot", &cid).await?;
            if cancel.is_cancelled() {
                return Ok(());
            }
            let receipt = serde_json::json!({"node_identity":node,"cid":cid}).to_string();
            if zip::verify_root(db, claim, &node, "hot", &cid, &receipt)
                .await
                .is_err()
            {
                durable::settle_failed(db, &snapshot, claim, "root_receipt_failed").await?;
                return Ok(());
            }
            durable::settle_verified(db, &snapshot, claim.clone(), node, cid).await?;
        }
        Ok(None) if cancel.is_cancelled() => {}
        Ok(None) => durable::settle_failed(db, &snapshot, claim, "invalid_manifest").await?,
        Err(error) => {
            if let Some(candidate) = error.candidate() {
                zip::retain_candidate(db, claim, &candidate.node_identity, "hot", &candidate.cid)
                    .await?;
            }
            if cancel.is_cancelled() {
                return Ok(());
            }
            let code = match error.reason() {
                DirectoryBuildError::PathConflict => "path_conflict",
                DirectoryBuildError::InvalidManifest => "invalid_manifest",
                DirectoryBuildError::BlockTooLarge => "directory_block_too_large",
                DirectoryBuildError::HashCollision => "directory_hash_collision",
                DirectoryBuildError::Canceled => return Ok(()),
                _ => "directory_build_failed",
            };
            durable::settle_failed(db, &snapshot, claim, code).await?;
        }
    }
    Ok(())
}
