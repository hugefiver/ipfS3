//! Complete locator candidates precede original-record validation. Failed
//! completeness checks are errors, never ordinary absence or a scan fallback.
use super::super::{ManagementError, Result, native, records};
use native::meter::Budget;
use records::locator::{AuthoritySelector, AuthorityView};
use sea_orm::ConnectionTrait;

pub(crate) async fn rebuild_sources(
    tx: &sea_orm::DatabaseTransaction,
    keys: &std::collections::BTreeSet<native::NativeKey>,
    work: records::DecodeWork<'_>,
) -> Result<()> {
    let mut budget = Budget::default();
    let result = records::locator::rebuild_sources(tx, keys, &mut budget).await;
    charge(&budget, work)?;
    result
}

pub(crate) async fn load<C: ConnectionTrait>(
    db: &C,
    selectors: &[AuthoritySelector],
    work: records::DecodeWork<'_>,
) -> Result<AuthorityView> {
    work.copy(selectors)?;
    let mut budget = Budget::default();
    let result = records::locator::discover_authority(db, selectors, &mut budget).await;
    // Charge storage work into the outer shared session, including failed reads.
    // A sub-call cannot reset the enclosing operation's work allowance.
    charge(&budget, work)?;
    result
}

pub(crate) fn charge(budget: &Budget, work: records::DecodeWork<'_>) -> Result<()> {
    work.with(|work| work.charge_storage(budget.work))
}

pub(crate) async fn actions_for_job<C: ConnectionTrait>(
    db: &C,
    job: &crate::store::entities::pin_job::Model,
    work: records::SharedDecodeBudget,
) -> Result<Vec<records::TrustedAppliedAction>> {
    let mut selectors = vec![AuthoritySelector::Job(job.id.clone())];
    if let Some(lease) = &job.lease_id {
        // Includes the original Submit for Poll and every migration/renewal
        // action on the lease; native continuity is validated by job_authority.
        selectors.push(AuthoritySelector::Lease(lease.clone()));
    }
    if let Some(target) = &job.target_id {
        selectors.push(AuthoritySelector::Target(target.clone()));
    }
    actions(db, &selectors, work).await
}

pub(crate) async fn actions_for_lease<C: ConnectionTrait>(
    db: &C,
    lease: &str,
    work: records::SharedDecodeBudget,
) -> Result<Vec<records::TrustedAppliedAction>> {
    actions(db, &[AuthoritySelector::Lease(lease.into())], work).await
}

pub(crate) async fn lease_participates<C: ConnectionTrait>(
    db: &C,
    lease: &str,
    work: records::DecodeWork<'_>,
) -> Result<bool> {
    let candidates = load(db, &[AuthoritySelector::Lease(lease.into())], work).await?;
    Ok(candidates.originals.iter().any(|(key, row)| {
        key.table == "pin_management_records"
            && native::value(row, "kind") == Some("action")
            && native::value(row, "state") == Some("applied")
            && native::value(row, "completed_at").is_some()
    }))
}

async fn actions<C: ConnectionTrait>(
    db: &C,
    selectors: &[AuthoritySelector],
    work: records::SharedDecodeBudget,
) -> Result<Vec<records::TrustedAppliedAction>> {
    let candidates = load(db, selectors, records::DecodeWork::shared(&work)).await?;
    let keys = candidates
        .originals
        .iter()
        .filter(|(key, row)| {
            key.table == "pin_management_records"
                && native::value(row, "kind") == Some("action")
                && native::value(row, "state") == Some("applied")
        })
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    records::DecodeWork::shared(&work).copy(&keys)?;
    let actions = records::actions_at_keys_with_work(db, &keys, work).await?;
    if actions.len() != keys.len() {
        return Err(ManagementError::Confirmation);
    }
    Ok(actions)
}
