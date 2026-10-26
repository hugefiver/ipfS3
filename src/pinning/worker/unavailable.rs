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
    store: &Store,
    coordinator: &PinningCoordinator,
    claimed: &ClaimedPinJob,
) -> AppResult<()> {
    let error = terminal_health_error();
    if claimed.model.operation == "submit" {
        let old_phase = claimed.model.submit_phase.as_deref().unwrap_or("running");
        jobs::retry_submit_recovery(
            txn.db(),
            claimed,
            Utc::now(),
            coordinator.settings().max_backoff,
            "provider unavailable",
        )
        .await?;
        txn.commit().await?;
        coordinate_claimed_one_target(store, coordinator, &claimed.model, Utc::now()).await?;
        transition(claimed, old_phase, "provider_unavailable_wait", None);
        return Ok(());
    }
    let now = Utc::now();
    let current = claimed.model.operation != "reconcile"
        || leases::guard_reconcile_remote_epoch(
            &txn,
            &claimed.model.provider,
            &claimed.model.cid,
            claimed
                .model
                .expected_remote_epoch
                .expect("validated remote scope"),
        )
        .await?;
    if current {
        let delay = provider_retry_delay(
            &claimed.model,
            &error,
            coordinator.settings().base_backoff,
            coordinator.settings().max_backoff,
        );
        jobs::retry_job(
            &txn,
            &claimed.model.id,
            claimed_lock(claimed)?,
            now,
            delay,
            delay,
            coordinator.settings().max_attempts,
            provider_error_label(error.class),
        )
        .await?;
    } else {
        complete_claimed_if_live(&txn, claimed, now).await?;
        ensure_current_reconcile(&txn, &claimed.model.provider, &claimed.model.cid, now).await?;
    }
    txn.commit().await?;
    coordinate_claimed_one_target(store, coordinator, &claimed.model, now).await?;
    transition(
        claimed,
        "running",
        if current {
            "retry_scheduled"
        } else {
            "stale_current_reconcile"
        },
        None,
    );
    Ok(())
}
