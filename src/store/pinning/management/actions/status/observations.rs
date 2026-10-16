//! Readonly is an operation class, not proof that an observation succeeded.
use super::*;
use crate::store::{
    entities::{
        pin_invocation_route, pin_replica_observation, pin_resource_history,
        pin_submit_observation, remote_pin_ledger,
    },
    pinning::legacy_migration,
};

pub(super) async fn needs_attention<C: ConnectionTrait>(
    db: &C,
    action: &records::CommittedAction,
    work: records::SharedDecodeBudget,
) -> Result<bool> {
    let context = records::DecodeWork::shared(&work);
    context.with(|work| work.nodes(1))?;
    let mut authorities = Vec::new();
    for seed in action
        .jobs
        .iter()
        .filter(|seed| seed.directive == records::JobDirective::Submit)
    {
        let (Some(lease), Some(target), Some(generation)) =
            (&seed.lease_id, &seed.target_id, seed.lease_generation)
        else {
            return Ok(true);
        };
        for _ in 0..2 {
            context.copy(&(lease, target, generation, &seed.cid))?;
        }
        let successors=records::current_models::<pin_job::Entity,_>(db,"FROM pin_jobs WHERE operation='poll' AND lease_id=$1 AND target_id=$2 AND expected_generation=$3 AND cid=$4","id",vec![lease.clone().into(),target.clone().into(),generation.into(),seed.cid.clone().into()],native::MAX_ROWS,context).await?;
        for successor in successors {
            context.copy(&(seed, &successor.id))?;
            let mut expected = seed.clone();
            expected.job_id = successor.id.clone();
            if job_authority::lookup_with_work(db, &successor, work.clone())
                .await?
                .as_ref()
                != Some(&expected)
            {
                return Ok(true);
            }
            if !action
                .jobs
                .iter()
                .chain(&authorities)
                .any(|authority| authority.job_id == expected.job_id)
            {
                context.with(|work| work.nodes(1))?;
                authorities.push(expected);
            }
        }
    }
    for authority in action.jobs.iter().chain(&authorities) {
        let Some(job) =
            records::current_model::<pin_job::Entity, _>(db, &[&authority.job_id], context).await?
        else {
            return Ok(true);
        };
        if job.last_error.is_some() || job.state == "running" && job.locked_until.is_none() {
            return Ok(true);
        }
        for _ in 0..2 {
            context.copy(&authority.job_id)?;
        }
        let fact=records::current_models::<pin_replica_observation::Entity,_>(db,"FROM pin_replica_observations WHERE job_id=$1 AND sequence=(SELECT MAX(sequence) FROM pin_replica_observations WHERE job_id=$1)","sequence",vec![authority.job_id.clone().into()],1,context).await?.pop();
        if let Some(fact) = fact {
            // A GET/DELETE report never changes the resource epoch or releases
            // quota. Only its exact operation-bound observation is consumed.
            if fact.expected_cid != authority.cid
                || fact.phase != job.operation
                || fact.remote_epoch < authority.remote_epoch
                || authority.directive != records::JobDirective::Submit
                    && fact.remote_epoch != authority.remote_epoch
                || context.with(|work| {
                    work.typed::<crate::pinning::identity::ProviderRouteSnapshot>(&fact.route)
                })? != authority.route
                || matches!(fact.outcome.as_str(), "Unknown" | "ReportedFailed")
            {
                return Ok(true);
            }
            let resources: Vec<crate::pinning::provider::ObservedResource> =
                context.with(|work| work.typed(&fact.resources))?;
            if resources.iter().any(|resource| {
                resource.resource_type != authority.route.resource_type()
                    || crate::pinning::provider::cids_equivalent(&resource.cid, &authority.cid).ok()
                        != Some(true)
                    || resource.status == crate::pinning::provider::ObservedResourceStatus::PinError
                    || authority.directive != records::JobDirective::Submit
                        && authority
                            .resource_id
                            .as_ref()
                            .is_some_and(|id| *id != resource.request_id)
            }) {
                return Ok(true);
            }
            if matches!(
                fact.outcome.as_str(),
                "ReportedQueued" | "ReportedPinning" | "ReportedPinned"
            ) && resources.is_empty()
            {
                return Ok(true);
            }
            if fact.safe_error.is_some() {
                let history = records::current_model::<
                    crate::store::pinning::jobs::history::Entity,
                    _,
                >(db, &[&job.id], context)
                .await?;
                let repaired = authority.directive == records::JobDirective::Submit
                    && job.state == "pending"
                    && job.submit_phase.as_deref() == Some("ready")
                    && matches!(fact.outcome.as_str(), "NotCreated" | "Rejected")
                    && history.is_some_and(|history| {
                        history.state == "active" && history.effect == "not_created"
                    });
                if !repaired {
                    return Ok(true);
                }
            }
        }
        for _ in 0..2 {
            context.copy(&authority.job_id)?;
        }
        let unresolved = records::bounded_projection(
            db,
            "FROM pin_submit_observations WHERE job_id=$1 AND needs_attention=TRUE",
            &[("job_id", "job_id")],
            "id",
            vec![authority.job_id.clone().into()],
            (native::MAX_ROWS, native::MAX_BYTES),
            context,
        )
        .await?
        .0;
        if !unresolved.is_empty() {
            return Ok(true);
        }
    }
    for authority in action
        .jobs
        .iter()
        .filter(|authority| authority.directive == records::JobDirective::ReadOnlyObserve)
    {
        let invocation = records::current_model::<pin_invocation_route::Entity, _>(
            db,
            &[&authority.job_id],
            context,
        )
        .await?;
        let Some(invocation) = invocation else {
            return Ok(true);
        };
        if invocation.remote_epoch != authority.remote_epoch
            || context.with(|work| {
                work.typed::<crate::pinning::identity::ProviderRouteSnapshot>(&invocation.route)
            })? != authority.route
        {
            return Ok(true);
        }
        context.copy(&[("provider", &authority.provider), ("cid", &authority.cid)])?;
        let mut key = std::collections::BTreeMap::from([
            ("provider".into(), authority.provider.clone()),
            ("cid".into(), authority.cid.clone()),
        ]);
        if authority.resource_key.table == "pin_resource_history" {
            key.insert("epoch".into(), authority.remote_epoch.to_string());
        }
        let provider = if let Some(binding) = legacy_migration::resolve_legacy_binding_with_work(
            db,
            &authority.resource_key.table,
            &key,
            &authority.provider,
            context,
        )
        .await?
        {
            if binding.historical_route.as_ref() != Some(&authority.route) {
                return Ok(true);
            }
            binding.operational_key
        } else {
            context.copy(&authority.provider)?;
            authority.provider.clone()
        };
        let row = if authority.resource_key.table == "pin_resource_history" {
            let epoch = authority.remote_epoch.to_string();
            context.copy(&epoch)?;
            let Some(history) = records::current_model::<pin_resource_history::Entity, _>(
                db,
                &[&provider, &authority.cid, &epoch],
                context,
            )
            .await?
            else {
                return Ok(true);
            };
            let envelope: HistoricalEnvelope = context.with(|work| work.typed(&history.ledger))?;
            if envelope.request_id != authority.resource_id {
                return Ok(true);
            }
            Some(envelope.ledger)
        } else {
            let Some(remote) = records::current_model::<remote_pin::Entity, _>(
                db,
                &[&provider, &authority.cid],
                context,
            )
            .await?
            else {
                return Ok(true);
            };
            if remote.epoch != authority.remote_epoch || remote.request_id != authority.resource_id
            {
                return Ok(true);
            }
            records::current_model::<remote_pin_ledger::Entity, _>(
                db,
                &[&provider, &authority.cid],
                context,
            )
            .await?
        };
        let Some(row) = row else { return Ok(true) };
        if row
            .route
            .as_deref()
            .map(|raw| {
                context
                    .with(|work| work.typed::<crate::pinning::identity::ProviderRouteSnapshot>(raw))
            })
            .transpose()?
            .as_ref()
            != Some(&authority.route)
            || matches!(row.effect.as_str(), "unknown" | "reported_unknown")
            || row.last_error.is_some()
        {
            return Ok(true);
        }
        let providers = legacy_migration::historical_keys_budgeted(
            db,
            "pin_submit_observations",
            &provider,
            Some(&authority.cid),
            context,
        )
        .await?;
        for provider in providers {
            for _ in 0..2 {
                context.copy(&(&provider, &authority.cid, authority.remote_epoch))?;
            }
            let unresolved=records::current_models::<pin_submit_observation::Entity,_>(db,"FROM pin_submit_observations WHERE provider=$1 AND expected_cid=$2 AND remote_epoch=$3 AND needs_attention=TRUE","id",vec![provider.into(),authority.cid.clone().into(),authority.remote_epoch.into()],native::MAX_ROWS,context).await?;
            for observation in unresolved {
                if context.with(|work| {
                    work.typed::<crate::pinning::identity::ProviderRouteSnapshot>(
                        &observation.route,
                    )
                })? == authority.route
                {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}
#[derive(serde::Deserialize)]
struct HistoricalEnvelope {
    ledger: remote_pin_ledger::Model,
    request_id: Option<String>,
}
