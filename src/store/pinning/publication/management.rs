//! Narrow existing-object allocation seam. Never publishes a version or opens a
//! transaction. Management owns admission, full-frame fences and job directives.
use super::*;

pub struct AbsoluteIntent<'a> {
    pub lease_id: &'a str,
    pub owner_object_id: &'a str,
    pub source: &'a str,
    pub intent: &'a LeaseIntent,
    pub targets: &'a [PinTargetSpec],
    pub started_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub requirements: &'a crate::pinning::management::Requirements,
}

pub async fn allocate_existing_in_transaction(
    tx: &DatabaseTransaction,
    request: AbsoluteIntent<'_>,
    limits: &ProviderLimitMap,
    now: DateTime<Utc>,
) -> AppResult<Vec<pin_lease_target::Model>> {
    let work = super::super::management::records::SharedDecodeBudget::default();
    allocate_existing_with_work(
        tx,
        request,
        limits,
        now,
        super::super::management::records::DecodeWork::shared(&work),
    )
    .await
}
pub(crate) async fn allocate_existing_with_work(
    tx: &DatabaseTransaction,
    request: AbsoluteIntent<'_>,
    limits: &ProviderLimitMap,
    now: DateTime<Utc>,
    work: super::super::management::records::DecodeWork<'_>,
) -> AppResult<Vec<pin_lease_target::Model>> {
    insert_absolute_lease_with_work(
        tx,
        request.lease_id,
        request.owner_object_id,
        request.source,
        request.intent,
        request.targets,
        request.started_at,
        request.expires_at,
        now,
        Some(request.requirements),
        work,
    )
    .await?;
    let mut assigned = BTreeMap::new();
    let mut affected = BTreeSet::new();
    let mut failed = BTreeSet::new();
    for target in request.targets {
        assign_target_providers_with_work(
            tx,
            request.lease_id,
            request.intent,
            target,
            limits,
            now,
            &mut assigned,
            &mut affected,
            &mut failed,
            work,
        )
        .await?;
    }
    // No implicit retry/reset/projection here: an old reserved row with no
    // invocation is unknown debt, not proof that a fresh POST is safe.
    allocated_targets(tx, request.lease_id, work).await
}

async fn allocated_targets(
    tx: &DatabaseTransaction,
    lease_id: &str,
    work: super::super::management::records::DecodeWork<'_>,
) -> AppResult<Vec<pin_lease_target::Model>> {
    use super::super::management::{admission::work_error, native, records};
    for _ in 0..2 {
        work.copy(lease_id).map_err(work_error)?;
    }
    records::current_models::<pin_lease_target::Entity, _>(
        tx,
        "FROM pin_lease_targets WHERE lease_id=$1",
        "created_at,id",
        vec![lease_id.into()],
        native::MAX_ROWS,
        work,
    )
    .await
    .map_err(work_error)
}

/// Attach one destination to an existing lease without changing its birth JSON,
/// generation, source references, start, expiry, or ownership.
pub async fn attach_destination_in_transaction(
    tx: &DatabaseTransaction,
    lease_id: &str,
    provider: &str,
    target: &PinTargetSpec,
    limits: &ProviderLimitMap,
    now: DateTime<Utc>,
) -> AppResult<pin_lease_target::Model> {
    let work = super::super::management::records::SharedDecodeBudget::default();
    attach_destination_with_work(
        tx,
        lease_id,
        provider,
        target,
        limits,
        now,
        super::super::management::records::DecodeWork::shared(&work),
    )
    .await
}
pub(crate) async fn attach_destination_with_work(
    tx: &DatabaseTransaction,
    lease_id: &str,
    provider: &str,
    target: &PinTargetSpec,
    limits: &ProviderLimitMap,
    now: DateTime<Utc>,
    work: super::super::management::records::DecodeWork<'_>,
) -> AppResult<pin_lease_target::Model> {
    let cid = ledger::allocation_cid_with_work(tx, provider, &target.cid, work).await?;
    let target = PinTargetSpec {
        cid,
        logical_size: target.logical_size,
    };
    use super::super::management::{admission::work_error, records};
    for _ in 0..2 {
        work.copy(&(lease_id, provider, &target.cid))
            .map_err(work_error)?;
    }
    if !records::current_models::<pin_lease_target::Entity, _>(
        tx,
        "FROM pin_lease_targets WHERE lease_id=$1 AND provider=$2 AND cid=$3",
        "created_at,id",
        vec![lease_id.into(), provider.into(), target.cid.clone().into()],
        super::super::management::native::MAX_ROWS,
        work,
    )
    .await
    .map_err(work_error)?
    .is_empty()
    {
        return Err(invalid_publication(
            "destination would merge two logical slots",
        ));
    }
    let mut affected = BTreeSet::new();
    let mut failed = BTreeSet::new();
    reserve_and_insert_target_with_work(
        tx,
        lease_id,
        provider,
        &target,
        limits,
        now,
        &mut affected,
        &mut failed,
        work,
    )
    .await?;
    work.copy(&(lease_id, provider, &target.cid, "::"))
        .map_err(work_error)?;
    let id = leases::target_id(lease_id, provider, &target.cid);
    records::current_model::<pin_lease_target::Entity, _>(tx, &[&id], work)
        .await
        .map_err(work_error)?
        .ok_or_else(|| invalid_publication("destination target missing"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::pinning::management::{
        native,
        records::{DecodeWork, SharedDecodeBudget},
    };

    #[tokio::test]
    async fn allocation_return_prepays_models_from_the_remaining_outer_work() {
        let fixture = crate::store::pinning::management::actions::tests::fixture_with_automatic(
            "allocation-return-work",
            true,
        )
        .await;
        let lease = pin_lease::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        let before = native::capture(&fixture.db).await.unwrap();
        let tx = fixture.db.begin().await.unwrap();
        let limited = SharedDecodeBudget::default();
        limited
            .lock()
            .unwrap()
            .consume(0, crate::pinning::management::wire::MAX_WIRE_BYTES - 8)
            .unwrap();
        assert!(matches!(
            allocated_targets(&tx, &lease.id, DecodeWork::shared(&limited)).await,
            Err(AppError::ManagementBudget)
        ));
        let ample = SharedDecodeBudget::default();
        let returned = allocated_targets(&tx, &lease.id, DecodeWork::shared(&ample))
            .await
            .unwrap();
        assert!(!returned.is_empty());
        assert!(returned.iter().all(|target| target.lease_id == lease.id));
        assert!(ample.lock().unwrap().usage().1 > 8);
        tx.rollback().await.unwrap();
        assert!(before == native::capture(&fixture.db).await.unwrap());
    }
}
