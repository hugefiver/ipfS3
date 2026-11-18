//! The worker's owned transaction between its business fences and effect write.
use super::*;
use crate::store::pinning::{
    legacy_migration,
    management::{self, records::DecodeWork},
};

pub(super) async fn record_request_receipt<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    route: &ProviderRouteSnapshot,
    remote: &crate::pinning::provider::RemotePin,
    now: DateTime<Utc>,
) -> AppResult<()> {
    use crate::pinning::provider::{ObservedResource, ObservedResourceStatus};
    let outcome = match remote.status {
        RemotePinStatus::Queued => "ReportedQueued",
        RemotePinStatus::Pinning => "ReportedPinning",
        RemotePinStatus::Pinned => "ReportedPinned",
        RemotePinStatus::Failed => return Ok(()),
    };
    let resource = submission::resource_evidence_for_route(
        route,
        ObservedResource {
            resource_type: route.resource_type(),
            cid: remote.cid.clone(),
            request_id: remote.request_id.clone(),
            status: ObservedResourceStatus::Reported,
            ownership: Ownership::Unknown,
            replica_evidence: None,
        },
    );
    let resources =
        serde_json::to_string(&[resource]).map_err(|e| AppError::Internal(e.to_string()))?;
    db.execute(sea_orm::Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO pin_submit_observations (id,job_id,submit_call,provider,expected_cid,route,remote_epoch,claim_until,started_at,observed_at,effect,resources,outcome,needs_attention) \
         SELECT h.job_id || ':call:' || CAST(h.submit_calls AS TEXT),h.job_id,h.submit_calls,$2,$3,i.route,i.remote_epoch,$4,h.started_at,$5,'observed',$6,$7,FALSE \
         FROM pin_submit_history h JOIN pin_invocation_routes i ON i.job_id=h.job_id WHERE h.job_id=$1 AND h.submit_calls>0 ON CONFLICT(job_id,submit_call) DO NOTHING",
        [job.id.clone().into(),job.provider.clone().into(),job.cid.clone().into(),job.locked_until.into(),now.into(),resources.into(),outcome.into()])).await?;
    Ok(())
}

fn effect(cleanup: bool) -> &'static str {
    if cleanup {
        "cleanup_pending"
    } else {
        "retained"
    }
}

pub(crate) async fn prepare_runtime_unpin_effect(
    tx: crate::store::RuntimeTransaction,
    provider: &str,
    cid: &str,
    work: DecodeWork<'_>,
) -> AppResult<(crate::store::RuntimeTransaction, bool)> {
    let catalog = tx.catalog(work);
    let cleanup = cleanup_allowed_catalog(catalog, provider, cid, work).await?;
    // Effect publication reads its own fresh binding and ledger row.
    let candidates = catalog
        .load(
            &legacy_migration::resource_receipt_selectors(provider, cid),
            work,
        )
        .await
        .map_err(management::admission::work_error)?;
    let row = get_from_catalog(provider, cid, &candidates, work, catalog).await?;
    write_effect(tx.db(), row, effect(cleanup)).await?;
    Ok((tx, cleanup))
}

pub(super) async fn historical_submission<C: ConnectionTrait>(
    db: &C,
    ledger: &remote_pin_ledger::Model,
    route: &ProviderRouteSnapshot,
    raw: &str,
    work: DecodeWork<'_>,
) -> AppResult<bool> {
    let Some(remote) = management::records::current_model::<
        crate::store::entities::remote_pin::Entity,
        _,
    >(db, &[&ledger.provider, &ledger.cid], work)
    .await
    .map_err(management::admission::work_error)?
    else {
        return Ok(false);
    };
    let Some(id) = remote.request_id.as_deref() else {
        return Ok(false);
    };
    let values = vec![
        ledger.provider.clone().into(),
        ledger.cid.clone().into(),
        raw.into(),
        remote.epoch.into(),
    ];
    let correlation = |request: &str| {
        crate::pinning::pinata::submission_correlation(request, &ledger.cid, &route.api_profile)
    };
    let promoted = id
        .starts_with(crate::pinning::pinata::V3_FILE_PREFIX)
        .then(|| correlation(id))
        .flatten();
    let rpc = submission::is_rpc(route);
    let mut receipt_values = values.clone();
    receipt_values.push(rpc.into());
    let row = db.query_one(sea_orm::Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT e.resources FROM pin_submit_observations e WHERE e.provider=$1 AND e.expected_cid=$2 AND e.route=$3 AND e.remote_epoch <= $4 AND e.effect='observed' AND (($5 AND e.outcome IN ('matched','accepted')) OR (NOT $5 AND e.outcome IN ('ReportedPinned','ReportedQueued','ReportedPinning'))) AND e.needs_attention=FALSE AND NOT EXISTS (SELECT 1 FROM pin_resource_history x WHERE x.provider=e.provider AND x.cid=e.expected_cid AND x.epoch>=e.remote_epoch AND x.epoch<$4) ORDER BY e.remote_epoch DESC,e.observed_at DESC,e.submit_call DESC LIMIT 1", receipt_values)).await?;
    if let Some(row) = row {
        let resources: String = row.try_get("", "resources")?;
        let evidence = work
            .with(|w| w.typed::<Vec<submission::ResourceEvidence>>(&resources))
            .map_err(management::admission::work_error)?;
        return Ok(!evidence.is_empty()
            && evidence.iter().all(|r| {
                (r.resource.request_id == id
                    || promoted.as_ref().is_some_and(|key| {
                        correlation(&r.resource.request_id).as_ref() == Some(key)
                    }))
                    && r.resource.resource_type == route.resource_type()
                    && (!rpc && r.resource.cid == ledger.cid
                        || r.key.as_ref().is_some_and(|key| {
                            key.backend == route.backend
                                && key.scope == route.scope
                                && key.resource_type == route.resource_type()
                                && key.resource_id == ledger.cid
                        }))
            }));
    }
    if let Some(key) = correlation(id) {
        let mut values = values;
        values.extend([
            key.into(),
            route.api_profile.replace('-', "_").into(),
            route.strategy.clone().into(),
        ]);
        return Ok(db.query_one(sea_orm::Statement::from_sql_and_values(db.get_database_backend(),
            "SELECT 1 FROM pin_jobs j JOIN pin_submit_history h ON h.job_id=j.id JOIN pin_invocation_routes i ON i.job_id=j.id WHERE j.provider=$1 AND j.cid=$2 AND j.operation='submit' AND i.route=$3 AND i.remote_epoch <= $4 AND COALESCE(h.correlation,j.id)=$5 AND h.api=$6 AND h.strategy=$7 AND h.effect='created' AND h.state='settled' AND h.submit_calls>0 AND h.recovery_queries=0 AND NOT EXISTS (SELECT 1 FROM pin_resource_history x WHERE x.provider=j.provider AND x.cid=j.cid AND x.epoch>=i.remote_epoch AND x.epoch<$4) LIMIT 1", values)).await?.is_some());
    }
    if route.resource_type() == crate::pinning::identity::RemoteResourceType::PsaRequest {
        // Older PSA envelopes lack a request field. A same-owner Poll identifier
        // captures that request exactly; a CID or settled Submit alone cannot.
        let row = db.query_one(sea_orm::Statement::from_sql_and_values(db.get_database_backend(),
            "SELECT p.id,p.target_id,p.expected_generation FROM pin_jobs j JOIN pin_submit_history h ON h.job_id=j.id JOIN pin_invocation_routes i ON i.job_id=j.id JOIN pin_jobs p ON p.target_id=j.target_id AND p.expected_generation=j.expected_generation AND p.provider=j.provider AND p.cid=j.cid AND p.operation='poll' JOIN pin_invocation_routes pi ON pi.job_id=p.id AND pi.route=i.route AND pi.remote_epoch>=i.remote_epoch AND pi.remote_epoch<=$4 WHERE j.provider=$1 AND j.cid=$2 AND j.operation='submit' AND i.route=$3 AND i.remote_epoch<=$4 AND h.api='psa' AND h.effect='created' AND h.state='settled' AND h.submit_calls=1 AND h.recovery_queries=0 AND NOT EXISTS (SELECT 1 FROM pin_resource_history x WHERE x.provider=j.provider AND x.cid=j.cid AND x.epoch>=i.remote_epoch AND x.epoch<$4) ORDER BY i.remote_epoch DESC,p.created_at DESC LIMIT 1", values)).await?;
        let Some(row) = row else { return Ok(false) };
        return Ok(row.try_get::<String>("", "id")?
            == crate::store::pinning::jobs::stable_poll_id(
                &ledger.provider,
                &ledger.cid,
                &row.try_get::<String>("", "target_id")?,
                row.try_get("", "expected_generation")?,
                id,
            ));
    }
    Ok(false)
}

#[cfg(test)]
mod tests;
