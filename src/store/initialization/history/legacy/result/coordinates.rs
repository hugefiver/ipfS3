use super::*;
use facts::{Outcome, RowRef};

pub(super) fn validate(
    facts: &ItemFacts,
    action: &RecordedAction,
    reads: &[Option<&NativeRow>],
    outputs: &[Option<NativeRow>],
    work: &mut records::ReadSession,
) -> Result<()> {
    if !matches!(
        (&action.receipt.binding.request.action, &facts.5),
        (
            model::Action::Create(_) | model::Action::Reapply(_) | model::Action::Backfill(_),
            Outcome::Allocate(..)
        ) | (model::Action::Renew { .. }, Outcome::Renew(..))
            | (
                model::Action::Retry | model::Action::Reconcile,
                Outcome::Recover(..)
            )
            | (model::Action::Migrate { .. }, Outcome::Migrate(..))
            | (model::Action::Cancel, Outcome::Cancel(..))
            | (
                model::Action::Retire | model::Action::RetireWith { .. },
                Outcome::Retire(..)
            )
    ) {
        return Err(ManagementError::Confirmation);
    }
    let get = |reference: RowRef| -> Result<(&facts::Key, Option<&NativeRow>)> {
        match reference {
            RowRef::Read(index) => Ok((
                &facts.1.get(index).ok_or(ManagementError::Confirmation)?.0,
                *reads.get(index).ok_or(ManagementError::Confirmation)?,
            )),
            RowRef::Output(index) => Ok((
                &facts.2.get(index).ok_or(ManagementError::Confirmation)?.0,
                outputs
                    .get(index)
                    .ok_or(ManagementError::Confirmation)?
                    .as_ref(),
            )),
        }
    };
    let mut coordinates = Vec::new();
    match &facts.5 {
        Outcome::Allocate(a, b, c, d, e) => {
            coordinates.extend([*a, *b, *c]);
            coordinates.extend(d);
            coordinates.extend(e);
        }
        Outcome::Renew(a, b, c, d, _) => {
            coordinates.extend([*a, *b]);
            coordinates.extend(c);
            coordinates.extend(d);
        }
        Outcome::Recover(a, b, c) => {
            coordinates.extend([*a, *b]);
            for (r, optional) in c {
                coordinates.push(*r);
                coordinates.extend(optional);
            }
        }
        Outcome::Migrate(a, b, c, d) => {
            coordinates.extend([*a, *b, *c]);
            coordinates.extend(d);
        }
        Outcome::Cancel(a, b) => coordinates.extend([*a, *b]),
        Outcome::Retire(a, b) => {
            coordinates.push(*a);
            coordinates.extend(b);
        }
    }
    for coordinate in coordinates {
        get(coordinate)?;
    }
    let mut births = BTreeSet::new();
    for birth in &facts.3 {
        let (job_key, job) = get(RowRef::Output(birth.0))?;
        let (route_key, route) = get(RowRef::Output(birth.1))?;
        let job = job.ok_or(ManagementError::Confirmation)?;
        let route = route.ok_or(ManagementError::Confirmation)?;
        if job_key.0 != "pin_jobs"
            || route_key.0 != "pin_invocation_routes"
            || job_key.1 != route_key.1
            || native::value(job, "id") != native::value(route, "job_id")
            || reads[facts.2[birth.0].1].is_some()
            || reads[facts.2[birth.1].1].is_some()
            || !births.insert((birth.0, birth.1))
        {
            return Err(ManagementError::Confirmation);
        }
    }
    let mut linked = BTreeSet::new();
    for link in &facts.4 {
        let authority = action
            .jobs
            .get(link.0)
            .ok_or(ManagementError::Confirmation)?;
        let (job_key, job) = get(link.1)?;
        let (route_key, route) = get(link.2)?;
        let job = job.ok_or(ManagementError::Confirmation)?;
        let route = route.ok_or(ManagementError::Confirmation)?;
        let epoch = protocol::cell(route, "remote_epoch")?
            .parse::<i64>()
            .map_err(|_| ManagementError::Confirmation)?;
        let recorded: crate::pinning::identity::ProviderRouteSnapshot =
            work.typed(protocol::cell(route, "route")?)?;
        let retained_poll = native::value(job, "operation") == Some("poll")
            && authority.directive == records::JobDirective::Submit
            && matches!(
                action.receipt.binding.request.action,
                model::Action::RetireWith {
                    selection: model::RetirementSelection::Migrate { .. }
                }
            );
        if job_key.0 != "pin_jobs"
            || route_key.0 != "pin_invocation_routes"
            || !linked.insert(link.0)
            || native::value(job, "id") != Some(&authority.job_id)
            || native::value(route, "job_id") != Some(&authority.job_id)
            || native::value(job, "provider") != Some(&authority.provider)
            || native::value(job, "cid") != Some(&authority.cid)
            || native::value(job, "target_id") != authority.target_id.as_deref()
            || native::value(job, "lease_id")
                .is_some_and(|value| Some(value) != authority.lease_id.as_deref())
            || native::value(job, "expected_generation")
                .is_some_and(|value| value.parse::<i64>().ok() != authority.lease_generation)
            || native::value(job, "expected_remote_epoch")
                .is_some_and(|value| value.parse::<i64>().ok() != Some(authority.remote_epoch))
            || epoch != authority.remote_epoch && (!retained_poll || epoch < authority.remote_epoch)
            || recorded != authority.route
            || !matches!(
                (authority.directive, native::value(job, "operation")),
                (records::JobDirective::Submit, Some("submit" | "poll"))
                    | (records::JobDirective::ReadOnlyObserve, Some("reconcile"))
                    | (records::JobDirective::ExistingCleanup, Some("unpin"))
            )
        {
            return Err(ManagementError::Confirmation);
        }
    }
    if linked.len() != action.jobs.len() {
        return Err(ManagementError::Confirmation);
    }
    Ok(())
}
