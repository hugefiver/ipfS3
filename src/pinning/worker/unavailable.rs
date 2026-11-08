use super::*;

pub(super) enum MissingRuntime {
    Finished,
    ExistingAdmission,
    Exhausted,
}

pub(super) fn settle_missing_runtime<'a>(
    store: &'a Store,
    coordinator: &'a PinningCoordinator,
    claimed: &'a ClaimedPinJob,
    work: SharedDecodeBudget,
) -> Pin<Box<dyn Future<Output = AppResult<MissingRuntime>> + Send + 'a>> {
    Box::pin(async move {
        let tx = store.begin_runtime(DecodeWork::shared(&work)).await?;
        if !jobs::fence_job_claim(&tx, &claimed.model.id, claimed_lock(claimed)?).await? {
            tx.rollback().await?;
            return Ok(MissingRuntime::Finished);
        }
        job_authority::fence_runtime_authority(&tx, &claimed.model, work.clone()).await?;
        {
            let observation = admission::worker_observation::claimed_runtime(
                &tx,
                &claimed.model,
                coordinator.provider_identity(&claimed.model.provider),
                work,
            )
            .await?;
            if observation.management_observation || observation.route.is_some() {
                drop(observation);
                tx.rollback().await?;
                return Ok(MissingRuntime::ExistingAdmission);
            }
            if observation.ordinary != ledger::JobAdmission::Allowed
                || !registered_poll_route_matches_on(&tx, &claimed.model).await?
            {
                drop(observation);
                jobs::park_identity_job(&tx, claimed).await?;
                tx.commit().await?;
                return Ok(MissingRuntime::Finished);
            }
        }
        if claimed.model.operation != "submit"
            && claimed.model.attempts >= coordinator.settings().max_attempts as i32
            && !(claimed.model.operation == "poll"
                && crate::store::entities::pin_replica_observation::receipt::is_cluster(
                    &tx,
                    &claimed.model.id,
                )
                .await?)
        {
            tx.rollback().await?;
            return Ok(MissingRuntime::Exhausted);
        }
        settle_ordinary(tx, store, coordinator, claimed).await?;
        Ok(MissingRuntime::Finished)
    })
}

// Both owners fence the exact claim and validate authority before handing off
// the transaction. Settlement commits before coordination opens another phase.
pub(super) async fn settle_ordinary(
    txn: crate::store::RuntimeTransaction,
    _store: &Store,
    _coordinator: &PinningCoordinator,
    claimed: &ClaimedPinJob,
) -> AppResult<()> {
    let result = jobs::terminate_claim(
        &txn,
        claimed,
        "current provider runtime unavailable",
        Utc::now(),
    )
    .await;
    match result {
        Ok(changed) => {
            txn.commit().await?;
            if changed {
                transition(claimed, "running", "error", None);
            }
            Ok(())
        }
        Err(error) => {
            txn.rollback().await?;
            Err(error)
        }
    }
}
