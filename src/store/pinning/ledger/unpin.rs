//! The worker's owned transaction between its business fences and effect write.
use super::*;
use crate::store::pinning::{
    legacy_migration,
    management::{self, records::DecodeWork},
};

pub(crate) async fn transport_handle<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    request_id: &str,
) -> AppResult<String> {
    let row = db.query_one(sea_orm::Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT p.file_id FROM pin_cleanup_ownership p JOIN pin_invocation_routes i ON i.route=p.route WHERE i.job_id=$1 AND p.provider=$2 AND p.cid=$3 AND p.request_id=$4 AND p.file_id IS NOT NULL",
        [job.id.clone().into(), job.provider.clone().into(), job.cid.clone().into(), request_id.into()])).await?;
    Ok(match row {
        Some(row) => row.try_get("", "file_id")?,
        None => request_id.to_owned(),
    })
}

pub(crate) async fn bind_file_handle(
    tx: &crate::store::RuntimeTransaction,
    job: &pin_job::Model,
    request_id: &str,
    file_id: &str,
    work: DecodeWork<'_>,
) -> AppResult<()> {
    let db = tx.db();
    let values = [
        file_id.into(),
        job.provider.clone().into(),
        job.cid.clone().into(),
        request_id.into(),
        job.id.clone().into(),
    ];
    let updated = db.execute(sea_orm::Statement::from_sql_and_values(db.get_database_backend(),
        "UPDATE pin_cleanup_ownership SET file_id=$1 WHERE provider=$2 AND cid=$3 AND request_id=$4 AND route=(SELECT route FROM pin_invocation_routes WHERE job_id=$5) AND file_id IS NULL", values.clone())).await?;
    if updated.rows_affected() == 0
        && cleanup_allowed_catalog(tx.catalog(work), &job.provider, &job.cid, work).await?
    {
        // A pre-upgrade Submit may be authorized only by its historical receipt.
        db.execute(sea_orm::Statement::from_sql_and_values(db.get_database_backend(),
            "INSERT INTO pin_cleanup_ownership(provider,cid,request_id,route,file_id) SELECT l.provider,l.cid,r.request_id,l.route,$1 FROM remote_pin_ledger l JOIN remote_pins r ON r.provider=l.provider AND r.cid=l.cid JOIN pin_invocation_routes i ON i.route=l.route WHERE l.provider=$2 AND l.cid=$3 AND r.request_id=$4 AND i.job_id=$5 ON CONFLICT(provider,cid) DO NOTHING",values)).await?;
    }
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
    if matches!(route.api_profile.as_str(), "pinata-legacy" | "pinata-v3") {
        let Some(correlation) =
            crate::pinning::pinata::submission_correlation(id, &ledger.cid, &route.api_profile)
        else {
            return Ok(false);
        };
        let mut values = values;
        values.push(correlation.into());
        values.push(route.api_profile.replace('-', "_").into());
        values.push(route.strategy.clone().into());
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
    let row = db.query_one(sea_orm::Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT e.resources FROM pin_submit_observations e WHERE e.provider=$1 AND e.expected_cid=$2 AND e.route=$3 AND e.remote_epoch <= $4 AND e.effect='observed' AND e.outcome IN ('matched','accepted') AND e.needs_attention=FALSE AND NOT EXISTS (SELECT 1 FROM pin_resource_history x WHERE x.provider=e.provider AND x.cid=e.expected_cid AND x.epoch>=e.remote_epoch AND x.epoch<$4) ORDER BY e.remote_epoch DESC,e.submit_call DESC LIMIT 1", values)).await?;
    let Some(row) = row else { return Ok(false) };
    let resources: String = row.try_get("", "resources")?;
    let evidence = work
        .with(|work| work.typed::<Vec<submission::ResourceEvidence>>(&resources))
        .map_err(management::admission::work_error)?;
    Ok(!evidence.is_empty()
        && evidence.iter().all(|r| {
            r.resource.request_id == id
                && r.resource.resource_type == route.resource_type()
                && r.key.as_ref().is_some_and(|key| {
                    key.backend == route.backend
                        && key.scope == route.scope
                        && key.resource_type == route.resource_type()
                        && key.resource_id == ledger.cid
                })
        }))
}

#[cfg(test)]
mod tests;
