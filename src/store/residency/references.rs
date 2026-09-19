use sea_orm::{
    ActiveValue::Set,
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseTransaction, EntityTrait,
    PaginatorTrait, QueryFilter, QueryOrder, QuerySelect,
    sea_query::{Expr, OnConflict},
};

use crate::{
    error::{AppError, AppResult},
    residency::{
        KuboTier, PhysicalResidencySnapshot, PhysicalVerification, ReferenceReason,
        ReferenceSummary, ResidencyLocation, ResolvedVersionResidency, StorageClass,
        VerificationState, VersionResidencyIdentity, model::corrupt,
    },
    store::{
        database_clock::database_now,
        entities::{
            object, object_version, physical_residency, pin_lease, pin_lease_target,
            residency_reference, version_residency,
        },
    },
};

const OWNER_VERSION: &str = "version";
const OWNER_TRANSITION: &str = "transition";
const ACTIVE_LEASE: &str = "active";
const ACTIVE_TARGET_STATES: [&str; 6] = [
    "waiting",
    "submitted",
    "pinned",
    "degraded",
    "quota_waiting",
    "quota_blocked",
];

/// Reserve shared hot physical rows in CID order after the publication's
/// ownership/version/lease frontier. This also orders inserts of previously
/// unseen CIDs, which a SELECT FOR UPDATE on missing rows cannot protect.
pub(crate) async fn prepare_hot_publication_frontier<C: ConnectionTrait>(
    txn: &C,
    mut cids: Vec<String>,
) -> AppResult<()> {
    cids.sort();
    cids.dedup();
    let now = database_now(txn).await?;
    for cid in cids {
        attach_physical_hot(txn, &cid, &PhysicalVerification::Pending, now).await?;
        if txn.get_database_backend() == DatabaseBackend::Postgres {
            physical_residency::Entity::find_by_id(("hot".to_owned(), cid))
                .lock_exclusive()
                .one(txn)
                .await?;
        }
    }
    Ok(())
}

/// Applies a verified hot receipt at the final publication boundary.
///
/// The caller must already hold the publication ownership/version/lease
/// frontiers and the hot physical row lock acquired by
/// `prepare_hot_publication_frontier`. A verified row can be reused only when
/// its node binding and receipt are identical; it is never rebound.
pub(crate) async fn apply_hot_publication_verification<C: ConnectionTrait>(
    txn: &C,
    cid: &str,
    verification: &PhysicalVerification,
) -> AppResult<()> {
    let PhysicalVerification::Verified {
        node_identity,
        receipt,
    } = verification
    else {
        return Err(AppError::InvalidArgument(
            "hot publication requires verified residency".to_owned(),
        ));
    };
    if node_identity.is_empty() || receipt.is_empty() {
        return Err(AppError::InvalidArgument(
            "verified residency requires node identity and receipt".to_owned(),
        ));
    }

    let existing = physical_residency::Entity::find_by_id((
        KuboTier::Hot.as_db_str().to_owned(),
        cid.to_owned(),
    ))
    .one(txn)
    .await?
    .ok_or_else(|| corrupt("hot publication frontier is missing"))?;
    let snapshot = physical_snapshot(existing)?;
    match snapshot.verification_state {
        VerificationState::Pending | VerificationState::Failed => {
            let now = database_now(txn).await?;
            mark_physical_verified(txn, cid, node_identity, receipt, now).await
        }
        VerificationState::Verified
            if snapshot.node_identity.as_deref() == Some(node_identity)
                && snapshot.verification_receipt.as_deref() == Some(receipt) =>
        {
            Ok(())
        }
        VerificationState::Verified => {
            Err(corrupt("conflicting hot physical verification receipt"))
        }
    }
}

pub async fn attach_hot_in_transaction<C: ConnectionTrait>(
    txn: &C,
    identity: &VersionResidencyIdentity,
    verification: &PhysicalVerification,
) -> AppResult<ResolvedVersionResidency> {
    validate_identity(identity)?;
    let (version, object) = lock_and_validate_version(txn, identity).await?;
    let now = database_now(txn).await?;
    attach_physical_hot(txn, &identity.cid, verification, now).await?;

    let residency = version_residency::Entity::find_by_id(&identity.version_row_id)
        .one(txn)
        .await?;
    if let Some(existing) = residency {
        validate_existing_version_residency(&existing, identity)?;
    } else {
        version_residency::Entity::insert(version_residency::ActiveModel {
            version_row_id: Set(version.id.clone()),
            object_id: Set(object.id.clone()),
            primary_tier: Set(KuboTier::Hot.as_db_str().to_owned()),
            storage_class: Set(StorageClass::Standard.as_db_str().to_owned()),
            cid: Set(object.cid.clone()),
            revision: Set(1),
            created_at: Set(now),
            updated_at: Set(now),
        })
        .exec(txn)
        .await?;
    }

    residency_reference::Entity::insert(residency_reference::ActiveModel {
        owner_kind: Set(OWNER_VERSION.to_owned()),
        owner_id: Set(version.id.clone()),
        reason: Set(ReferenceReason::RetainedVersion.as_db_str().to_owned()),
        version_row_id: Set(version.id),
        object_id: Set(object.id),
        tier: Set(KuboTier::Hot.as_db_str().to_owned()),
        cid: Set(object.cid),
        created_at: Set(now),
    })
    .on_conflict(
        OnConflict::columns([
            residency_reference::Column::OwnerKind,
            residency_reference::Column::OwnerId,
            residency_reference::Column::Reason,
            residency_reference::Column::Tier,
            residency_reference::Column::Cid,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(txn)
    .await?;

    resolve_version_residency(txn, &identity.version_row_id).await
}

pub async fn resolve_version_residency<C: ConnectionTrait>(
    db: &C,
    version_row_id: &str,
) -> AppResult<ResolvedVersionResidency> {
    if version_row_id.is_empty() {
        return Err(corrupt("empty version identity"));
    }
    let residency = version_residency::Entity::find_by_id(version_row_id)
        .one(db)
        .await?
        .ok_or_else(|| corrupt("content version has no residency"))?;
    let version = object_version::Entity::find_by_id(version_row_id)
        .one(db)
        .await?
        .ok_or_else(|| corrupt("residency owner version is missing"))?;
    if version.kind != "object" || version.object_id.as_deref() != Some(&residency.object_id) {
        return Err(corrupt("residency owner is not the exact content version"));
    }
    let object = object::Entity::find_by_id(&residency.object_id)
        .one(db)
        .await?
        .ok_or_else(|| corrupt("residency object is missing"))?;
    if object.cid != residency.cid || object.bucket != version.bucket || object.key != version.key {
        return Err(corrupt(
            "residency identity does not match immutable object metadata",
        ));
    }

    let tier = KuboTier::from_db_str(&residency.primary_tier)?;
    let storage_class = StorageClass::from_db_str(&residency.storage_class)?;
    if !matches!(
        (tier, storage_class),
        (KuboTier::Hot, StorageClass::Standard) | (KuboTier::Cold, StorageClass::StandardIa)
    ) || residency.revision <= 0
    {
        return Err(corrupt("invalid primary residency"));
    }
    let physical = physical_residency::Entity::find_by_id((
        residency.primary_tier.clone(),
        residency.cid.clone(),
    ))
    .one(db)
    .await?
    .ok_or_else(|| corrupt("primary physical residency is missing"))?;
    let physical = physical_snapshot(physical)?;

    Ok(ResolvedVersionResidency {
        identity: VersionResidencyIdentity::new(
            residency.version_row_id,
            residency.object_id,
            residency.cid,
        ),
        primary: ResidencyLocation::new(tier, physical.location.cid.clone()),
        storage_class,
        revision: residency.revision,
        physical,
    })
}

pub async fn release_version_reference_in_transaction<C: ConnectionTrait>(
    txn: &C,
    version_row_id: &str,
) -> AppResult<bool> {
    let residency = version_residency::Entity::find_by_id(version_row_id)
        .one(txn)
        .await?
        .ok_or_else(|| corrupt("cannot release a version with no residency"))?;
    let deleted = residency_reference::Entity::delete_many()
        .filter(residency_reference::Column::OwnerKind.eq(OWNER_VERSION))
        .filter(residency_reference::Column::OwnerId.eq(version_row_id))
        .filter(
            residency_reference::Column::Reason.eq(ReferenceReason::RetainedVersion.as_db_str()),
        )
        .filter(residency_reference::Column::VersionRowId.eq(version_row_id))
        .filter(residency_reference::Column::ObjectId.eq(residency.object_id))
        .filter(residency_reference::Column::Tier.eq(residency.primary_tier))
        .filter(residency_reference::Column::Cid.eq(residency.cid))
        .exec(txn)
        .await?;
    Ok(deleted.rows_affected == 1)
}

pub async fn attach_transition_hold_in_transaction(
    txn: &DatabaseTransaction,
    transition_id: &str,
    reason: ReferenceReason,
    identity: &VersionResidencyIdentity,
    location: &ResidencyLocation,
) -> AppResult<bool> {
    if transition_id.is_empty()
        || !matches!(
            reason,
            ReferenceReason::TransitionStaging | ReferenceReason::TransitionCleanupHold
        )
    {
        return Err(AppError::InvalidArgument(
            "invalid transition residency hold".to_owned(),
        ));
    }
    validate_identity(identity)?;
    lock_and_validate_version(txn, identity).await?;
    if physical_residency::Entity::find_by_id((
        location.tier.as_db_str().to_owned(),
        location.cid.clone(),
    ))
    .one(txn)
    .await?
    .is_none()
    {
        return Err(corrupt("transition hold physical residency is missing"));
    }
    let now = database_now(txn).await?;
    let inserted = residency_reference::Entity::insert(residency_reference::ActiveModel {
        owner_kind: Set(OWNER_TRANSITION.to_owned()),
        owner_id: Set(transition_id.to_owned()),
        reason: Set(reason.as_db_str().to_owned()),
        version_row_id: Set(identity.version_row_id.clone()),
        object_id: Set(identity.object_id.clone()),
        tier: Set(location.tier.as_db_str().to_owned()),
        cid: Set(location.cid.clone()),
        created_at: Set(now),
    })
    .on_conflict(
        OnConflict::columns([
            residency_reference::Column::OwnerKind,
            residency_reference::Column::OwnerId,
            residency_reference::Column::Reason,
            residency_reference::Column::Tier,
            residency_reference::Column::Cid,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(txn)
    .await?;
    Ok(inserted == 1)
}

pub async fn release_transition_hold_in_transaction(
    txn: &DatabaseTransaction,
    transition_id: &str,
    reason: ReferenceReason,
    location: &ResidencyLocation,
) -> AppResult<bool> {
    if transition_id.is_empty()
        || !matches!(
            reason,
            ReferenceReason::TransitionStaging | ReferenceReason::TransitionCleanupHold
        )
    {
        return Err(AppError::InvalidArgument(
            "invalid transition residency hold".to_owned(),
        ));
    }
    let deleted = residency_reference::Entity::delete_many()
        .filter(residency_reference::Column::OwnerKind.eq(OWNER_TRANSITION))
        .filter(residency_reference::Column::OwnerId.eq(transition_id))
        .filter(residency_reference::Column::Reason.eq(reason.as_db_str()))
        .filter(residency_reference::Column::Tier.eq(location.tier.as_db_str()))
        .filter(residency_reference::Column::Cid.eq(&location.cid))
        .exec(txn)
        .await?;
    Ok(deleted.rows_affected == 1)
}

/// Locks residency rows in deterministic version/reference/physical order.
/// Callers must acquire existing version, owner, and lease frontiers first.
pub async fn lock_residency_frontier(
    txn: &DatabaseTransaction,
    version_row_ids: &[String],
    locations: &[ResidencyLocation],
) -> AppResult<()> {
    let mut version_row_ids = version_row_ids.to_vec();
    version_row_ids.sort();
    version_row_ids.dedup();
    let mut locations = locations.to_vec();
    locations.sort();
    locations.dedup();

    for version_row_id in &version_row_ids {
        let query = version_residency::Entity::find_by_id(version_row_id);
        if txn.get_database_backend() == DatabaseBackend::Postgres {
            query.lock_exclusive().one(txn).await?;
        } else {
            query.one(txn).await?;
        }
    }
    if !version_row_ids.is_empty() {
        let mut query = residency_reference::Entity::find()
            .filter(residency_reference::Column::VersionRowId.is_in(version_row_ids))
            .order_by_asc(residency_reference::Column::VersionRowId)
            .order_by_asc(residency_reference::Column::OwnerKind)
            .order_by_asc(residency_reference::Column::OwnerId)
            .order_by_asc(residency_reference::Column::Reason)
            .order_by_asc(residency_reference::Column::Tier)
            .order_by_asc(residency_reference::Column::Cid);
        if txn.get_database_backend() == DatabaseBackend::Postgres {
            query = query.lock_exclusive();
        }
        query.all(txn).await?;
    }
    for location in &locations {
        let query = physical_residency::Entity::find_by_id((
            location.tier.as_db_str().to_owned(),
            location.cid.clone(),
        ));
        if txn.get_database_backend() == DatabaseBackend::Postgres {
            query.lock_exclusive().one(txn).await?;
        } else {
            query.one(txn).await?;
        }
    }
    Ok(())
}

/// Returns authoritative known references for observability and fencing only.
/// No count returned by this function authorizes deletion or unpinning.
pub async fn reference_summary_in_transaction(
    txn: &DatabaseTransaction,
    location: &ResidencyLocation,
) -> AppResult<ReferenceSummary> {
    let count_reason = |reason: ReferenceReason| {
        residency_reference::Entity::find()
            .filter(residency_reference::Column::Tier.eq(location.tier.as_db_str()))
            .filter(residency_reference::Column::Cid.eq(&location.cid))
            .filter(residency_reference::Column::Reason.eq(reason.as_db_str()))
            .count(txn)
    };
    let retained_versions = count_reason(ReferenceReason::RetainedVersion).await?;
    let transition_staging_holds = count_reason(ReferenceReason::TransitionStaging).await?;
    let transition_cleanup_holds = count_reason(ReferenceReason::TransitionCleanupHold).await?;

    // Provider leases describe hot retention, never proof of a cold copy.
    // Expiry is deliberately state-based until the authoritative lease worker
    // ends the lease; quota delays must not remove its protection.
    let (active_lease_targets, active_lease_owners) = if location.tier == KuboTier::Hot {
        let targets = pin_lease_target::Entity::find()
            .inner_join(pin_lease::Entity)
            .filter(pin_lease_target::Column::Cid.eq(&location.cid))
            .filter(pin_lease_target::Column::State.is_in(ACTIVE_TARGET_STATES))
            .filter(pin_lease::Column::State.eq(ACTIVE_LEASE))
            .count(txn)
            .await?;
        let owners = pin_lease::Entity::find()
            .inner_join(object::Entity)
            .filter(object::Column::Cid.eq(&location.cid))
            .filter(pin_lease::Column::State.eq(ACTIVE_LEASE))
            .count(txn)
            .await?;
        (targets, owners)
    } else {
        (0, 0)
    };

    Ok(ReferenceSummary {
        retained_versions,
        transition_staging_holds,
        transition_cleanup_holds,
        active_lease_targets,
        active_lease_owners,
    })
}

pub async fn mark_hot_verified_in_transaction(
    txn: &DatabaseTransaction,
    identity: &VersionResidencyIdentity,
    node_identity: &str,
    receipt: &str,
) -> AppResult<bool> {
    if node_identity.is_empty() || receipt.is_empty() {
        return Err(AppError::InvalidArgument(
            "verified residency requires node identity and receipt".to_owned(),
        ));
    }
    lock_and_validate_version(txn, identity).await?;
    let residency = version_residency::Entity::find_by_id(&identity.version_row_id)
        .one(txn)
        .await?
        .ok_or_else(|| corrupt("verification owner has no residency"))?;
    validate_existing_version_residency(&residency, identity)?;
    let now = database_now(txn).await?;
    let updated = physical_residency::Entity::update_many()
        .col_expr(
            physical_residency::Column::VerificationState,
            Expr::value("verified"),
        )
        .col_expr(
            physical_residency::Column::NodeIdentity,
            Expr::value(Some(node_identity.to_owned())),
        )
        .col_expr(
            physical_residency::Column::VerificationReceipt,
            Expr::value(Some(receipt.to_owned())),
        )
        .col_expr(
            physical_residency::Column::VerifiedAt,
            Expr::value(Some(now)),
        )
        .col_expr(physical_residency::Column::UpdatedAt, Expr::value(now))
        .filter(physical_residency::Column::Tier.eq(KuboTier::Hot.as_db_str()))
        .filter(physical_residency::Column::Cid.eq(&identity.cid))
        .filter(physical_residency::Column::VerificationState.is_in(["pending", "failed"]))
        .exec(txn)
        .await?;
    if updated.rows_affected == 1 {
        return Ok(true);
    }
    let existing = physical_residency::Entity::find_by_id((
        KuboTier::Hot.as_db_str().to_owned(),
        identity.cid.clone(),
    ))
    .one(txn)
    .await?
    .ok_or_else(|| corrupt("verification physical residency is missing"))?;
    if existing.verification_state == "verified"
        && existing.node_identity.as_deref() == Some(node_identity)
        && existing.verification_receipt.as_deref() == Some(receipt)
    {
        Ok(false)
    } else {
        Err(corrupt("conflicting physical verification receipt"))
    }
}

async fn lock_and_validate_version<C: ConnectionTrait>(
    txn: &C,
    identity: &VersionResidencyIdentity,
) -> AppResult<(object_version::Model, object::Model)> {
    let query = object_version::Entity::find_by_id(&identity.version_row_id);
    let version = if txn.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(txn).await?
    } else {
        query.one(txn).await?
    }
    .ok_or_else(|| corrupt("residency owner version is missing"))?;
    if version.kind != "object" || version.object_id.as_deref() != Some(&identity.object_id) {
        return Err(corrupt(
            "delete marker or mismatched version cannot own residency",
        ));
    }
    let object = object::Entity::find_by_id(&identity.object_id)
        .one(txn)
        .await?
        .ok_or_else(|| corrupt("residency object is missing"))?;
    if object.cid != identity.cid || object.bucket != version.bucket || object.key != version.key {
        return Err(corrupt(
            "residency identity does not match immutable object metadata",
        ));
    }
    Ok((version, object))
}

async fn attach_physical_hot<C: ConnectionTrait>(
    txn: &C,
    cid: &str,
    verification: &PhysicalVerification,
    now: chrono::DateTime<chrono::Utc>,
) -> AppResult<()> {
    let (state, node_identity, receipt, verified_at) = match verification {
        PhysicalVerification::Pending => ("pending", None, None, None),
        PhysicalVerification::Verified {
            node_identity,
            receipt,
        } if !node_identity.is_empty() && !receipt.is_empty() => (
            "verified",
            Some(node_identity.clone()),
            Some(receipt.clone()),
            Some(now),
        ),
        PhysicalVerification::Verified { .. } => {
            return Err(AppError::InvalidArgument(
                "verified residency requires node identity and receipt".to_owned(),
            ));
        }
    };
    physical_residency::Entity::insert(physical_residency::ActiveModel {
        tier: Set(KuboTier::Hot.as_db_str().to_owned()),
        cid: Set(cid.to_owned()),
        node_identity: Set(node_identity.clone()),
        verification_state: Set(state.to_owned()),
        verification_receipt: Set(receipt.clone()),
        verified_at: Set(verified_at),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .on_conflict(
        OnConflict::columns([
            physical_residency::Column::Tier,
            physical_residency::Column::Cid,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(txn)
    .await?;

    let existing = physical_residency::Entity::find_by_id((
        KuboTier::Hot.as_db_str().to_owned(),
        cid.to_owned(),
    ))
    .one(txn)
    .await?
    .ok_or_else(|| corrupt("hot physical residency attach failed"))?;
    match verification {
        PhysicalVerification::Pending => physical_snapshot(existing).map(|_| ()),
        PhysicalVerification::Verified {
            node_identity,
            receipt,
        } if existing.verification_state == "verified"
            && existing.node_identity.as_deref() == Some(node_identity)
            && existing.verification_receipt.as_deref() == Some(receipt) =>
        {
            physical_snapshot(existing).map(|_| ())
        }
        PhysicalVerification::Verified {
            node_identity,
            receipt,
        } if existing.verification_state == "pending" => {
            mark_physical_verified(txn, cid, node_identity, receipt, now).await
        }
        PhysicalVerification::Verified { .. } => {
            Err(corrupt("conflicting hot physical verification receipt"))
        }
    }
}

async fn mark_physical_verified<C: ConnectionTrait>(
    txn: &C,
    cid: &str,
    node_identity: &str,
    receipt: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> AppResult<()> {
    let updated = physical_residency::Entity::update_many()
        .col_expr(
            physical_residency::Column::VerificationState,
            Expr::value("verified"),
        )
        .col_expr(
            physical_residency::Column::NodeIdentity,
            Expr::value(Some(node_identity.to_owned())),
        )
        .col_expr(
            physical_residency::Column::VerificationReceipt,
            Expr::value(Some(receipt.to_owned())),
        )
        .col_expr(
            physical_residency::Column::VerifiedAt,
            Expr::value(Some(now)),
        )
        .col_expr(physical_residency::Column::UpdatedAt, Expr::value(now))
        .filter(physical_residency::Column::Tier.eq(KuboTier::Hot.as_db_str()))
        .filter(physical_residency::Column::Cid.eq(cid))
        .filter(physical_residency::Column::VerificationState.is_in(["pending", "failed"]))
        .exec(txn)
        .await?;
    if updated.rows_affected == 1 {
        Ok(())
    } else {
        Err(corrupt("hot physical verification compare-and-set failed"))
    }
}

fn validate_identity(identity: &VersionResidencyIdentity) -> AppResult<()> {
    if identity.version_row_id.is_empty()
        || identity.object_id.is_empty()
        || identity.cid.is_empty()
    {
        Err(AppError::InvalidArgument(
            "residency identity fields must not be empty".to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn validate_existing_version_residency(
    existing: &version_residency::Model,
    identity: &VersionResidencyIdentity,
) -> AppResult<()> {
    if existing.object_id == identity.object_id
        && existing.cid == identity.cid
        && existing.primary_tier == KuboTier::Hot.as_db_str()
        && existing.storage_class == StorageClass::Standard.as_db_str()
        && existing.revision > 0
    {
        Ok(())
    } else {
        Err(corrupt("conflicting version residency attach"))
    }
}

fn physical_snapshot(row: physical_residency::Model) -> AppResult<PhysicalResidencySnapshot> {
    let tier = KuboTier::from_db_str(&row.tier)?;
    let verification_state = VerificationState::from_db_str(&row.verification_state)?;
    let valid_shape = match verification_state {
        VerificationState::Verified => {
            row.node_identity
                .as_deref()
                .is_some_and(|value| !value.is_empty())
                && row
                    .verification_receipt
                    .as_deref()
                    .is_some_and(|value| !value.is_empty())
                && row.verified_at.is_some()
        }
        VerificationState::Pending | VerificationState::Failed => {
            row.node_identity.is_none()
                && row.verification_receipt.is_none()
                && row.verified_at.is_none()
        }
    };
    if !valid_shape {
        return Err(corrupt("invalid physical verification shape"));
    }
    Ok(PhysicalResidencySnapshot {
        location: ResidencyLocation::new(tier, row.cid),
        verification_state,
        node_identity: row.node_identity,
        verification_receipt: row.verification_receipt,
        verified_at: row.verified_at,
    })
}
