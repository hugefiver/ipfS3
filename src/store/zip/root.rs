use chrono::{Duration, Utc};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction,
    EntityTrait, QueryFilter, Set, TransactionTrait, sea_query::OnConflict,
};

use super::{invalid, lock_batch, manifest, required, stale};
use crate::{
    error::AppResult,
    store::{
        database_clock::database_now,
        entities::{zip_batch, zip_root_build, zip_root_reference},
    },
};

#[derive(Clone, Debug)]
pub struct RootClaim {
    pub batch_id: String,
    pub revision: i64,
    pub epoch: i64,
    pub worker: String,
}

/// Start an intent BEFORE any root RPC. A takeover retains earlier intents and
/// candidates; expired workers cannot validate or publish into the new epoch.
pub async fn claim_root(
    db: &DatabaseConnection,
    id: &str,
    worker: &str,
    lease_seconds: i64,
) -> AppResult<RootClaim> {
    if !required(worker) || !(1..=3600).contains(&lease_seconds) {
        return Err(invalid());
    }
    let tx = db.begin().await?;
    let batch = lock_batch(&tx, id).await?;
    if !batch.manifest_prepared
        || matches!(
            batch.root_status.as_str(),
            "complete" | "partial" | "disabled" | "empty"
        )
        || (batch.state == "published" && batch.root_status != "failed")
        || !manifest::entries(&tx, id)
            .await?
            .iter()
            .any(|item| item.cid.is_some())
    {
        return Err(stale());
    }
    let now = database_now(&tx).await?;
    // A published/failed batch still owns its live root lease. Only an
    // expired (or explicitly released) lease permits a new epoch/revision.
    if batch.root_revision > 0 {
        let previous = zip_root_build::Entity::find_by_id((
            id.to_owned(),
            batch.root_revision,
            batch.root_epoch,
        ))
        .one(&tx)
        .await?
        .ok_or_else(stale)?;
        if previous.lease_until > now {
            return Err(stale());
        }
    }
    let revision = if batch.state == "published" {
        batch.root_revision.checked_add(1).ok_or_else(invalid)?
    } else {
        batch.root_revision.max(1)
    };
    let epoch = batch.root_epoch.checked_add(1).ok_or_else(invalid)?;
    let until = now + Duration::seconds(lease_seconds);
    zip_root_build::Entity::insert(zip_root_build::ActiveModel {
        batch_id: Set(id.to_owned()),
        revision: Set(revision),
        epoch: Set(epoch),
        worker: Set(worker.to_owned()),
        lease_until: Set(until),
        status: Set("intent".into()),
        error_code: Set(None),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .exec(&tx)
    .await?;
    zip_batch::ActiveModel {
        root_revision: Set(revision),
        root_epoch: Set(epoch),
        updated_at: Set(now),
        ..batch.into()
    }
    .update(&tx)
    .await?;
    tx.commit().await?;
    Ok(RootClaim {
        batch_id: id.to_owned(),
        revision,
        epoch,
        worker: worker.to_owned(),
    })
}

pub(super) async fn current_claim(
    tx: &DatabaseTransaction,
    claim: &RootClaim,
) -> AppResult<zip_root_build::Model> {
    // Call only AFTER acquiring the batch row lock.
    let build =
        zip_root_build::Entity::find_by_id((claim.batch_id.clone(), claim.revision, claim.epoch))
            .one(tx)
            .await?
            .ok_or_else(stale)?;
    let batch = zip_batch::Entity::find_by_id(&claim.batch_id)
        .one(tx)
        .await?
        .ok_or_else(stale)?;
    if batch.root_revision != claim.revision
        || batch.root_epoch != claim.epoch
        || build.worker != claim.worker
        || build.lease_until <= database_now(tx).await?
    {
        return Err(stale());
    }
    Ok(build)
}

pub async fn renew_claim(
    db: &DatabaseConnection,
    claim: &RootClaim,
    seconds: i64,
) -> AppResult<()> {
    if !(1..=3600).contains(&seconds) {
        return Err(invalid());
    }
    let tx = db.begin().await?;
    lock_batch(&tx, &claim.batch_id).await?;
    let build = current_claim(&tx, claim).await?;
    if build.status == "unknown" || build.status == "failed" {
        return Err(stale());
    }
    let now = database_now(&tx).await?;
    zip_root_build::ActiveModel {
        lease_until: Set(now + Duration::seconds(seconds)),
        updated_at: Set(now),
        ..build.into()
    }
    .update(&tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub async fn mark_invoked(db: &DatabaseConnection, claim: &RootClaim) -> AppResult<()> {
    let tx = db.begin().await?;
    lock_batch(&tx, &claim.batch_id).await?;
    let build = current_claim(&tx, claim).await?;
    if !matches!(build.status.as_str(), "intent" | "reconciling") {
        return Err(stale());
    }
    zip_root_build::ActiveModel {
        status: Set("invoked".into()),
        updated_at: Set(Utc::now()),
        ..build.into()
    }
    .update(&tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Reconcile an earlier unknown/root candidate by read-only external probing.
/// The worker must verify the full DAG/pin again before recording a receipt;
/// do not claim the prior epoch's receipt as proof for this one.
pub async fn mark_reconciling(db: &DatabaseConnection, claim: &RootClaim) -> AppResult<()> {
    let tx = db.begin().await?;
    lock_batch(&tx, &claim.batch_id).await?;
    let build = current_claim(&tx, claim).await?;
    if build.status != "intent" {
        return Err(stale());
    }
    zip_root_build::ActiveModel {
        status: Set("reconciling".into()),
        updated_at: Set(Utc::now()),
        ..build.into()
    }
    .update(&tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Lost external Commit/HTTP response: persist unknown BEFORE releasing claim;
/// a future worker must reconcile the intent/known CID, not presume success.
pub async fn mark_unknown(db: &DatabaseConnection, claim: &RootClaim) -> AppResult<()> {
    let tx = db.begin().await?;
    lock_batch(&tx, &claim.batch_id).await?;
    let build = current_claim(&tx, claim).await?;
    if build.status != "invoked" {
        return Err(stale());
    }
    let now = database_now(&tx).await?;
    zip_root_build::ActiveModel {
        status: Set("unknown".into()),
        lease_until: Set(now),
        updated_at: Set(now),
        ..build.into()
    }
    .update(&tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Record an externally returned CID even if the claim was superseded. This
/// never adopts or removes it; an old worker cannot overwrite current state.
pub async fn retain_candidate(
    db: &DatabaseConnection,
    claim: &RootClaim,
    node: &str,
    tier: &str,
    cid: &str,
) -> AppResult<()> {
    if !required(node) || !required(cid) || !matches!(tier, "hot" | "cold") {
        return Err(invalid());
    }
    let tx = db.begin().await?;
    lock_batch(&tx, &claim.batch_id).await?;
    let build =
        zip_root_build::Entity::find_by_id((claim.batch_id.clone(), claim.revision, claim.epoch))
            .one(&tx)
            .await?
            .ok_or_else(stale)?;
    if build.worker != claim.worker
        || !matches!(
            build.status.as_str(),
            "reconciling" | "invoked" | "unknown" | "verified"
        )
    {
        return Err(stale());
    }
    let now = Utc::now();
    zip_root_reference::Entity::insert(zip_root_reference::ActiveModel {
        batch_id: Set(claim.batch_id.clone()),
        revision: Set(claim.revision),
        epoch: Set(claim.epoch),
        node_identity: Set(node.into()),
        tier: Set(tier.into()),
        cid: Set(cid.into()),
        state: Set("retained".into()),
        verification_receipt: Set(None),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .on_conflict(
        OnConflict::columns([
            zip_root_reference::Column::BatchId,
            zip_root_reference::Column::Revision,
            zip_root_reference::Column::Epoch,
            zip_root_reference::Column::NodeIdentity,
            zip_root_reference::Column::Tier,
            zip_root_reference::Column::Cid,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(&tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn reference<C: ConnectionTrait>(
    db: &C,
    claim: &RootClaim,
    node: &str,
    tier: &str,
    cid: &str,
) -> AppResult<zip_root_reference::Model> {
    zip_root_reference::Entity::find_by_id((
        claim.batch_id.clone(),
        claim.revision,
        claim.epoch,
        node.to_owned(),
        tier.to_owned(),
        cid.to_owned(),
    ))
    .one(db)
    .await?
    .ok_or_else(stale)
}

/// Only call after the worker independently verifies recursive pin and full DAG.
/// The receipt is durable but is not a substitute for the worker's external check.
pub async fn verify_root(
    db: &DatabaseConnection,
    claim: &RootClaim,
    node: &str,
    tier: &str,
    cid: &str,
    receipt: &str,
) -> AppResult<()> {
    if !required(receipt) {
        return Err(invalid());
    }
    let tx = db.begin().await?;
    lock_batch(&tx, &claim.batch_id).await?;
    let build = current_claim(&tx, claim).await?;
    if !matches!(build.status.as_str(), "invoked" | "reconciling") {
        return Err(stale());
    }
    let found = reference(&tx, claim, node, tier, cid).await?;
    if found.verification_receipt.is_some() {
        return Err(stale());
    }
    zip_root_reference::ActiveModel {
        verification_receipt: Set(Some(receipt.into())),
        updated_at: Set(Utc::now()),
        ..found.into()
    }
    .update(&tx)
    .await?;
    zip_root_build::ActiveModel {
        status: Set("verified".into()),
        updated_at: Set(Utc::now()),
        ..build.into()
    }
    .update(&tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Includes unknown, invoked, and verified intents; candidate refs come from
/// `snapshot`. A process restart never substitutes an in-memory status.
pub async fn recovery(db: &DatabaseConnection, id: &str) -> AppResult<Vec<zip_root_build::Model>> {
    let builds = zip_root_build::Entity::find()
        .filter(zip_root_build::Column::BatchId.eq(id))
        .all(db)
        .await?;
    Ok(builds
        .into_iter()
        .filter(|b| b.status != "failed")
        .collect())
}

/// Exact owner AND physical node/tier/CID existence; a same-CID different-node
/// reference does not prove ownership. `absent` is not evidence that Kubo GC ran.
pub async fn root_existence(
    db: &DatabaseConnection,
    id: &str,
    node: &str,
    tier: &str,
    cid: &str,
) -> AppResult<&'static str> {
    let refs = zip_root_reference::Entity::find()
        .filter(zip_root_reference::Column::BatchId.eq(id))
        .filter(zip_root_reference::Column::NodeIdentity.eq(node))
        .filter(zip_root_reference::Column::Tier.eq(tier))
        .filter(zip_root_reference::Column::Cid.eq(cid))
        .all(db)
        .await?;
    Ok(if refs.iter().any(|r| r.state == "adopted") {
        "adopted"
    } else if !refs.is_empty() {
        "retained"
    } else {
        "absent"
    })
}
