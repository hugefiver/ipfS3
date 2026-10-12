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
    insert_absolute_lease(tx, request.lease_id, request.owner_object_id, request.source,
        request.intent, request.targets, request.started_at, request.expires_at, now, Some(request.requirements)).await?;
    let mut assigned = BTreeMap::new();
    let mut affected = BTreeSet::new();
    let mut failed = BTreeSet::new();
    for target in request.targets {
        assign_target_providers(tx, request.lease_id, request.intent, target, limits, now,
            &mut assigned, &mut affected, &mut failed).await?;
    }
    // No implicit retry/reset/projection here: an old reserved row with no
    // invocation is unknown debt, not proof that a fresh POST is safe.
    Ok(pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(request.lease_id))
        .order_by_asc(pin_lease_target::Column::CreatedAt)
        .order_by_asc(pin_lease_target::Column::Id).all(tx).await?)
}

/// Attach one destination to an existing lease without changing its birth JSON,
/// generation, source references, start, expiry, or ownership.
pub async fn attach_destination_in_transaction(
    tx: &DatabaseTransaction, lease_id: &str, provider: &str, target: &PinTargetSpec,
    limits: &ProviderLimitMap, now: DateTime<Utc>,
) -> AppResult<pin_lease_target::Model> {
    let cid = ledger::allocation_cid(tx, provider, &target.cid).await?;
    let target = PinTargetSpec { cid, logical_size:target.logical_size };
    if pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
        .filter(pin_lease_target::Column::Provider.eq(provider))
        .filter(pin_lease_target::Column::Cid.eq(&target.cid)).one(tx).await?.is_some()
    { return Err(invalid_publication("destination would merge two logical slots")); }
    let mut affected = BTreeSet::new();
    let mut failed = BTreeSet::new();
    reserve_and_insert_target(tx, lease_id, provider, &target, limits, now, &mut affected, &mut failed).await?;
    pin_lease_target::Entity::find_by_id(leases::target_id(lease_id, provider, &target.cid))
        .one(tx).await?.ok_or_else(|| invalid_publication("destination target missing"))
}
