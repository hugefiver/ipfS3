//! A late Submit can identify a remote for an ordinary Reconcile, never revive its owner.
use super::*;
use crate::store::{
    RuntimeTransaction,
    pinning::{
        leases,
        management::{actions::job_authority, records::DecodeWork},
    },
};

async fn cleanup_request(
    tx: &RuntimeTransaction,
    row: &evidence::Model,
    job: &pin_job::Model,
    remote: &remote_pin::Model,
    work: DecodeWork<'_>,
) -> AppResult<Option<String>> {
    if remote.request_id.is_some()
        || !matches!(row.outcome.as_str(), "matched" | "accepted")
        || !row.needs_attention
        || row.provider != job.provider
        || job.expected_remote_epoch.is_some()
        || row.id != format!("{}:call:{}", job.id, row.submit_call)
    {
        return Ok(None);
    }
    let Some(request) = matching_request(row) else {
        return Ok(None);
    };
    let route = work
        .with(|w| w.typed::<ProviderRouteSnapshot>(&row.route))
        .map_err(job_authority::safe)?;
    let valid_handle = match route.resource_type() {
        RemoteResourceType::RpcPin => {
            crate::pinning::ipfs_rpc::observed_handle_matches(&route, &request, &row.expected_cid)
        }
        RemoteResourceType::ClusterPin => {
            canonical_resource_cid(&row.expected_cid).ok().as_ref() == Some(&request)
        }
        _ => false,
    };
    if !valid_handle {
        return Ok(None);
    }
    let (Some(lease), Some(target), Some(generation)) =
        (&job.lease_id, &job.target_id, job.expected_generation)
    else {
        return Ok(None);
    };
    let catalog = tx.catalog(work);
    if job.id != jobs::stable_submit_id(&job.provider, &job.cid, target, generation)
        || !receipt_resource_context(
            tx.db(),
            row,
            job,
            remote,
            Some(&request),
            Some((catalog, work)),
        )
        .await?
        || job_authority::lookup_catalog(catalog, job, work.budget().clone())
            .await
            .map_err(job_authority::safe)?
            .is_some()
        || leases::target_holds_cleanup(&catalog, &job.provider, &job.cid, work).await?
    {
        return Ok(None);
    }
    let ended = tx.db().query_one(Statement::from_sql_and_values(tx.db().get_database_backend(),
        "SELECT 1 FROM pin_leases l JOIN pin_lease_targets t ON t.lease_id=l.id \
         WHERE l.id=$1 AND t.id=$2 AND t.provider=$3 AND t.cid=$4 AND l.generation>$5 \
         AND l.state IN ('expired','cancelled','evicted') AND t.state='released' \
         AND NOT EXISTS (SELECT 1 FROM pin_leases a JOIN pin_lease_targets live ON live.lease_id=a.id \
           WHERE a.state='active' AND live.provider=$3 AND live.cid=$4 AND live.state IN ('waiting','submitted','pinned','degraded'))",
        [lease.clone().into(),target.clone().into(),job.provider.clone().into(),job.cid.clone().into(),generation.into()])).await?;
    if ended.is_none() || has_other_debt(tx, row, job, work).await? {
        return Ok(None);
    }
    Ok(Some(request))
}

/// Receipt completion and restart recovery only offer the remote job a turn;
/// it must recheck the receipt under its own claim.
pub(crate) async fn wake(
    tx: &RuntimeTransaction,
    row: &evidence::Model,
    job: &pin_job::Model,
    now: DateTime<Utc>,
    work: DecodeWork<'_>,
) -> AppResult<()> {
    let Some(remote) = remote_pin::Entity::find_by_id((job.provider.clone(), job.cid.clone()))
        .one(tx.db())
        .await?
    else {
        return Ok(());
    };
    if cleanup_request(tx, row, job, &remote, work)
        .await?
        .is_some()
    {
        jobs::wake_late_receipt_reconcile(tx, &remote, now, work).await?;
    }
    Ok(())
}

/// Replay only the scheduler hint after a crash between the fact and wake commits.
pub(crate) async fn recover_wake(store: &crate::store::Store, id: &str) -> AppResult<()> {
    let budget = crate::store::pinning::management::records::SharedDecodeBudget::default();
    let work = DecodeWork::shared(&budget);
    let tx = store.begin_runtime(work).await?;
    if let Some(job) = jobs::lock_submit_receipt(tx.db(), id).await?
        && let Some(row) = latest_rpc_with_work(tx.db(), id, work)
            .await
            .map_err(job_authority::safe)?
    {
        wake(&tx, &row, &job, Utc::now(), work).await?;
    }
    tx.commit().await
}

/// Acquire the original Submit/history before the remote and Reconcile, as
/// receipt arrival does. Including in-flight rows serializes a concurrent park
/// with the wake even when the first lookup precedes the receipt commit.
pub(crate) async fn settle(
    tx: &RuntimeTransaction,
    claimed: &ClaimedPinJob,
    work: DecodeWork<'_>,
) -> AppResult<Option<String>> {
    let reconcile = &claimed.model;
    let Some(epoch) = reconcile.expected_remote_epoch else {
        return Ok(None);
    };
    if reconcile.id != jobs::stable_reconcile_id(&reconcile.provider, &reconcile.cid, epoch) {
        return Ok(None);
    }
    let ids = evidence::Entity::find()
        .select_only()
        .column(evidence::Column::JobId)
        .filter(evidence::Column::Provider.eq(&reconcile.provider))
        .filter(evidence::Column::ExpectedCid.eq(&reconcile.cid))
        .filter(evidence::Column::Outcome.is_in(["in_flight", "matched", "accepted"]))
        .filter(evidence::Column::NeedsAttention.eq(true))
        .limit(2)
        .into_tuple::<String>()
        .all(tx.db())
        .await?;
    let [id] = ids.as_slice() else {
        return Ok(None);
    };
    let Some(job) = jobs::lock_submit_receipt(tx.db(), id).await? else {
        return Ok(None);
    };
    let Some(row) = latest_rpc_with_work(tx.db(), id, work)
        .await
        .map_err(job_authority::safe)?
    else {
        return Ok(None);
    };
    if !matches!(row.outcome.as_str(), "matched" | "accepted") {
        return Ok(None);
    }
    let Some(remote) = lock_receipt_remote(tx.db(), &reconcile.provider, &reconcile.cid).await?
    else {
        return Ok(None);
    };
    if epoch != remote.epoch {
        return Ok(None);
    }
    let Some(claim) = reconcile.locked_until else {
        return Ok(None);
    };
    if !jobs::fence_job_claim(tx.db(), &reconcile.id, claim).await? {
        return Ok(None);
    }
    crate::store::pinning::management::records::locator::fence_authority(tx.db())
        .await
        .map_err(job_authority::safe)?;
    let catalog = tx.catalog(work);
    if job_authority::lookup_catalog(catalog, reconcile, work.budget().clone())
        .await
        .map_err(job_authority::safe)?
        .is_some()
    {
        return Ok(None);
    }
    let Some(request) = cleanup_request(tx, &row, &job, &remote, work).await? else {
        return Ok(None);
    };
    publish(tx, &row, &remote, &request).await?;
    Ok(Some(request))
}

async fn has_other_debt(
    tx: &RuntimeTransaction,
    row: &evidence::Model,
    job: &pin_job::Model,
    work: DecodeWork<'_>,
) -> AppResult<bool> {
    let catalog = tx.catalog(work);
    let snapshot =
        AdmissionSnapshot::read_with_catalog(&job.provider, Some(&job.cid), catalog, &work, false)
            .await?;
    if snapshot.unreadable.contains(&job.cid)
        || snapshot.unwitnessed_submissions.contains(&job.cid)
        || snapshot
            .evidence
            .iter()
            .any(|other| other.id != row.id && other.needs_attention)
        || snapshot
            .histories
            .values()
            .any(|h| h.job_id != job.id && h.effect == "unknown")
        || snapshot
            .ledgers
            .get(&job.cid)
            .is_none_or(|l| !matches!(l.effect.as_str(), "unknown" | "reserved" | "confirmed"))
    {
        return Ok(true);
    }
    // An unresolved upload for another CID may have reported this root.
    let keys = crate::store::pinning::legacy_migration::historical_keys_with_catalog(
        "pin_submit_observations",
        &job.provider,
        None,
        work,
        catalog,
    )
    .await
    .map_err(job_authority::safe)?;
    let reports = evidence::Entity::find()
        .select_only()
        .column(evidence::Column::Resources)
        .filter(evidence::Column::Provider.is_in(keys))
        .filter(evidence::Column::ExpectedCid.ne(&job.cid))
        .filter(evidence::Column::NeedsAttention.eq(true))
        .filter(evidence::Column::Resources.ne("[]"))
        .into_tuple::<String>()
        .all(tx.db())
        .await?;
    Ok(reports
        .iter()
        .any(|reports| reported_cids(reports).any(|cid| cid == job.cid)))
}

async fn publish(
    tx: &RuntimeTransaction,
    row: &evidence::Model,
    remote: &remote_pin::Model,
    request: &str,
) -> AppResult<()> {
    let history = jobs::history::Entity::update_many()
        .col_expr(jobs::history::Column::Effect, Expr::value("created"))
        .col_expr(jobs::history::Column::State, Expr::value("settled"))
        .filter(jobs::history::Column::JobId.eq(&row.job_id))
        .filter(jobs::history::Column::SubmitCalls.eq(row.submit_call))
        .exec(tx.db())
        .await?;
    let relation = fact_relation(tx.db()).await?;
    let settled = tx.db().execute(Statement::from_sql_and_values(
        tx.db().get_database_backend(),
        format!("UPDATE {} SET needs_attention=FALSE WHERE id=$1 AND job_id=$2 AND submit_call=$3 \
                 AND provider=$4 AND expected_cid=$5 AND route=$6 AND remote_epoch=$7 AND claim_until=$8 \
                 AND resources=$9 AND observed_at=$10 AND outcome=$11 AND effect='observed' AND needs_attention=TRUE", relation.table),
        [row.id.clone().into(), row.job_id.clone().into(), row.submit_call.into(), row.provider.clone().into(),
         row.expected_cid.clone().into(), row.route.clone().into(), row.remote_epoch.into(), row.claim_until.into(),
         row.resources.clone().into(), row.observed_at.into(), row.outcome.clone().into()],
    )).await?;
    let attached = remote_pin::Entity::update_many()
        .col_expr(remote_pin::Column::RequestId, Expr::value(request))
        .filter(remote_pin::Column::Provider.eq(&remote.provider))
        .filter(remote_pin::Column::Cid.eq(&remote.cid))
        .filter(remote_pin::Column::Epoch.eq(remote.epoch))
        .filter(remote_pin::Column::RequestId.is_null())
        .exec(tx.db())
        .await?;
    if history.rows_affected != 1 || settled.rows_affected() != 1 || attached.rows_affected != 1 {
        return Err(AppError::Database(
            "late Submit cleanup evidence changed".into(),
        ));
    }
    // The ended target cannot project this receipt; Reconcile now owns its cleanup.
    pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value("done"))
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::value(Option::<DateTime<Utc>>::None),
        )
        .col_expr(pin_job::Column::UpdatedAt, Expr::value(Utc::now()))
        .filter(pin_job::Column::Id.eq(&row.job_id))
        .filter(pin_job::Column::State.is_in(["pending", "running"]))
        .exec(tx.db())
        .await?;
    Ok(())
}
