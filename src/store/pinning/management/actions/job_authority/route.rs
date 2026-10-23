//! Resolve historical dispatch routes from the complete job authority closure.
use super::*;
use crate::store::entities::pin_lease_target;

pub(crate) async fn historical_route<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
) -> AppResult<Option<ProviderRouteSnapshot>> {
    let work = records::SharedDecodeBudget::default();
    let actions = crate::store::pinning::management::admission::authority::actions_for_job(
        db,
        job,
        work.clone(),
    )
    .await
    .map_err(safe)?;
    if let Some(authority) = lookup_from_actions(db, job, &actions, work.clone())
        .await
        .map_err(safe)?
    {
        return Ok(Some(authority.route));
    }
    migration_route(db, job, &actions, work).await
}

/// Resolve the permit and any management migration fallback from one complete
/// action closure in a continuous admission read. Permits are freshly derived.
pub(crate) async fn admission_with_route<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    work: records::SharedDecodeBudget,
) -> AppResult<(Option<JobPermit>, Option<ProviderRouteSnapshot>)> {
    let actions = crate::store::pinning::management::admission::authority::actions_for_job(
        db,
        job,
        work.clone(),
    )
    .await
    .map_err(safe)?;
    let authority = lookup_from_actions(db, job, &actions, work.clone())
        .await
        .map_err(safe)?;
    let permit = check_with_authority(db, job, authority, work.clone())
        .await
        .map_err(safe)?;
    let route = if permit.is_none() {
        migration_route(db, job, &actions, work).await?
    } else {
        None
    };
    Ok((permit, route))
}

async fn migration_route<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    actions: &[records::TrustedAppliedAction],
    work: records::SharedDecodeBudget,
) -> AppResult<Option<ProviderRouteSnapshot>> {
    // This is complete, sealed lease/job membership, not a current-provider hint.
    // Without any migration points there is no management route to resolve;
    // ordinary callers still validate their legacy route and retirement fences.
    if !actions
        .iter()
        .any(|action| !action.action_view().migration_points.is_empty())
    {
        return Ok(None);
    }
    let (Some(lease), Some(target_id)) = (&job.lease_id, &job.target_id) else {
        return Ok(None);
    };
    let Some(birth) =
        crate::store::entities::pin_lease_requirement::contract::get(db, lease).await?
    else {
        return Ok(None);
    };
    let candidates =
        super::super::points::candidates_for_cid_with_work(db, lease, &job.cid, &birth, work)
            .await
            .map_err(safe)?;
    let mut proven = None;
    for (_, allocation, point) in candidates {
        let Some(point) = point else {
            continue;
        };
        if point.destination_target_id != *target_id || allocation != job.provider {
            continue;
        }
        let target = pin_lease_target::Entity::find_by_id(target_id.clone())
            .one(db)
            .await?
            .ok_or_else(|| {
                AppError::InvalidPinningRequest("management invocation target is missing".into())
            })?;
        let captured = ledger::job_route(db, &job.id).await?.ok_or_else(|| {
            AppError::InvalidPinningRequest("management invocation route is missing".into())
        })?;
        if target.lease_id != *lease
            || target.provider != job.provider
            || target.cid != job.cid
            || !(captured == point.destination_route
                || (point.source_allocation == point.destination_allocation
                    && captured == point.source_resource_route))
            || proven.is_some()
        {
            return Err(AppError::InvalidPinningRequest(
                "management invocation point changed".into(),
            ));
        }
        proven = Some(captured);
    }
    Ok(proven)
}
