//! Explicit DB staging is separate from zero-write dry-run. Wire is a locator
//! and reviewed public proposal, never the source of original private evidence.
use super::*;
use crate::pinning::management::{
    self as management, DryRunPlan, PlanStatus, Snapshot, local_config::ManagementConfig,
    wire::ImportedPlan,
};
use sea_orm::{DatabaseBackend, DatabaseConnection, IsolationLevel, TransactionTrait};
mod global;
pub(super) use global::confirm as confirm_global;
pub use global::{ReadyPublication, confirm_ready};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanLocator {
    pub plan_id: String,
}

pub(super) async fn lock(tx: &DatabaseTransaction) -> Result<()> {
    if tx.get_database_backend() == DatabaseBackend::Sqlite {
        super::native::scope::writer_boundary(tx).await?;
    } else {
        // This protects only foundation records. B/C still own business locks.
        tx.execute_unprepared("LOCK TABLE pin_management_records,pin_management_retirements IN SHARE ROW EXCLUSIVE MODE NOWAIT").await.map_err(super::super::nowait_error)?;
    }
    Ok(())
}

pub async fn stage_confirmation(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    auth: &LocalAuthorization,
    request: &PlanRequest,
    snapshot: &Snapshot,
    plan: &DryRunPlan,
    ttl: chrono::Duration,
) -> Result<PlanLocator> {
    stage_with_work(
        db,
        config,
        auth,
        (request, snapshot, plan),
        ttl,
        SharedDecodeBudget::default(),
    )
    .await
}
pub(super) async fn stage_with_work(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    auth: &LocalAuthorization,
    (request, snapshot, plan): (&PlanRequest, &Snapshot, &DryRunPlan),
    ttl: chrono::Duration,
    work: SharedDecodeBudget,
) -> Result<PlanLocator> {
    auth.check(db).await?;
    management::validate_request(request).map_err(|_| ManagementError::Confirmation)?;
    reject_converted_unexecuted(db, auth, request).await?;
    if ttl <= chrono::Duration::zero()
        || ttl > chrono::Duration::minutes(15)
        || plan
            .items()
            .iter()
            .any(|item| matches!(item.status, PlanStatus::Blocked(_)))
    {
        return Err(ManagementError::Confirmation);
    }
    let tx = db
        .begin_with_config(Some(IsolationLevel::RepeatableRead), None)
        .await
        .map_err(|_| ManagementError::Database)?;
    lock(&tx).await?;
    let current = management::db_snapshot::collect_in_transaction_with_session(
        &tx,
        config,
        request,
        work.clone(),
    )
    .await
    .map_err(snapshot_error)?
    .planning_snapshot()
    .map_err(|_| ManagementError::Changed)?;
    if snapshot.native_frame != current.native_frame
        || snapshot.configuration != current.configuration
        || snapshot.objects != current.objects
        || snapshot.routes != current.routes
        || snapshot.targets != current.targets
        || snapshot.remotes != current.remotes
        || snapshot.historical_remotes != current.historical_remotes
        || snapshot.unknown_resources != current.unknown_resources
        || snapshot.ledger_domains != current.ledger_domains
        || snapshot.inventories != current.inventories
        || snapshot.root_references != current.root_references
        || snapshot.applied != current.applied
        || management::plan::dry_run_with_work(request, &current, work.clone()).map_err(
            |error| {
                if error == management::SafeError::LimitExceeded {
                    ManagementError::Budget
                } else {
                    ManagementError::Changed
                }
            },
        )? != *plan
    {
        return Err(ManagementError::Changed);
    }
    let now = crate::store::database_clock::database_now(&tx)
        .await
        .map_err(|_| ManagementError::Database)?;
    let expires_at = now
        .checked_add_signed(ttl)
        .ok_or(ManagementError::Expired)?;
    let frame = snapshot
        .native_frame
        .as_ref()
        .ok_or(ManagementError::Confirmation)?;
    if frame.database_identity != auth.database_identity
        || frame.database_scope != auth.database_scope
    {
        return Err(ManagementError::Unauthorized);
    }
    let before = archive::encode_proposal(auth, request, snapshot, plan, now, expires_at, &work)?;
    let anchor = plan
        .items()
        .first()
        .ok_or(ManagementError::Confirmation)?
        .request
        .item_id
        .as_str();
    let reference = archive::encode_locator(
        &before,
        &mut *work.lock().map_err(|_| ManagementError::InvalidEvidence)?,
    )?;
    let draft = archive::encode_draft(
        &before,
        &mut *work.lock().map_err(|_| ManagementError::InvalidEvidence)?,
    )?;
    // Immutable original proposal is retained in native_before, even after the
    // prepared row becomes confirmed/applied. Same IDs cannot recapture a body.
    for item in &request.items {
        let native_before = if item.item_id == anchor {
            &before
        } else {
            &reference
        };
        DecodeWork::shared(&work).copy(&(
            &auth.database_scope,
            &request.plan_id,
            &item.item_id,
            "action",
            &auth.principal,
            &auth.database_identity,
            &draft,
            native_before,
            "[]",
            "[]",
            "prepared",
        ))?;
        insert_in_transaction(
            &tx,
            auth,
            RecordWrite {
                plan_id: &request.plan_id,
                item_id: &item.item_id,
                kind: RecordKind::Action,
                binding: &draft,
                native_before,
                result: None,
                intent_ids: &[],
                job_ids: &[],
                state: RecordState::Prepared,
                expires_at,
            },
        )
        .await?;
    }
    let source_keys = request
        .items
        .iter()
        .map(|item| native::NativeKey {
            table: super::super::schema::RECORDS.into(),
            values: vec![
                auth.database_scope.clone(),
                request.plan_id.clone(),
                item.item_id.clone(),
                "action".into(),
            ],
        })
        .collect::<Vec<_>>();
    seal_originals(&tx, &source_keys, DecodeWork::shared(&work)).await?;
    tx.commit().await.map_err(|_| ManagementError::Database)?;
    Ok(PlanLocator {
        plan_id: request.plan_id.clone(),
    })
}

fn snapshot_error(error: management::db_snapshot::SnapshotError) -> ManagementError {
    if error == management::db_snapshot::SnapshotError::LimitExceeded {
        ManagementError::Budget
    } else {
        ManagementError::Changed
    }
}

async fn reject_converted_unexecuted<C: ConnectionTrait>(
    db: &C,
    auth: &LocalAuthorization,
    request: &PlanRequest,
) -> Result<()> {
    let mut budget = native::meter::Budget::default();
    for item in &request.items {
        let key = native::NativeKey {
            table: super::super::schema::RECORDS.into(),
            values: vec![
                auth.database_scope.clone(),
                request.plan_id.clone(),
                item.item_id.clone(),
                "action".into(),
            ],
        };
        // An unused plan ID has no source generation to recover. Check the
        // persistent control too: deletion/tombstone cannot become fresh absence.
        let source_key = locator::source_key(&key)?;
        if db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
            "SELECT 1 FROM pin_management_source_control WHERE source_key=$1 UNION ALL SELECT 1 FROM pin_management_records WHERE database_scope=$2 AND plan_id=$3 AND item_id=$4 AND kind='action'",
            [source_key.into(), auth.database_scope.clone().into(), request.plan_id.clone().into(), item.item_id.clone().into()]))
            .await.map_err(|_| ManagementError::Database)?.is_none() {
            continue;
        }
        if crate::store::initialization::history::requires_new_confirmation(db, &key, &mut budget)
            .await?
        {
            return Err(ManagementError::Confirmation);
        }
    }
    Ok(())
}

pub async fn confirm_imported(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    auth: &LocalAuthorization,
    imported: &ImportedPlan,
) -> Result<ConfirmedBinding> {
    if !imported.untrusted_applied_claims().is_empty() {
        return Err(ManagementError::Confirmation);
    }
    let reviewed = imported
        .confirmation()
        .ok_or(ManagementError::Confirmation)?;
    confirm_staged(
        db,
        config,
        auth,
        imported.request(),
        reviewed,
        Some(imported),
        imported.session(),
    )
    .await
}
pub(super) async fn confirm_staged(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    auth: &LocalAuthorization,
    request: &PlanRequest,
    reviewed: &[String],
    imported: Option<&ImportedPlan>,
    work: SharedDecodeBudget,
) -> Result<ConfirmedBinding> {
    auth.check(db).await?;
    management::validate_request(request).map_err(|_| ManagementError::Confirmation)?;
    reject_converted_unexecuted(db, auth, request).await?;
    let tx = db
        .begin_with_config(Some(IsolationLevel::RepeatableRead), None)
        .await
        .map_err(|_| ManagementError::Database)?;
    lock(&tx).await?;
    let source = evidence::Evidence::load_with_work(
        &tx,
        &auth.database_scope,
        &auth.database_identity,
        std::slice::from_ref(&request.plan_id),
        work.clone(),
    )
    .await?;
    source.budget()?.nodes(
        1 + source
            .archive_members(&auth.database_scope, &request.plan_id)
            .count(),
    )?;
    let rows = source
        .archive_members(&auth.database_scope, &request.plan_id)
        .collect::<Vec<_>>();
    let first = rows.first().ok_or(ManagementError::Confirmation)?;
    let review = archive::load_review(&source, first, &mut *source.budget()?)?;
    if let Some(imported) = imported {
        super::imported::validate_reviewed(&review, imported)?;
    }
    if reviewed.iter().enumerate().any(|(index, id)| {
        reviewed[..index].contains(id)
            || !review
                .entries()
                .iter()
                .any(|entry| entry.status == PlanStatus::Candidate && entry.request.item_id == *id)
    }) || review.entries().iter().any(|entry| {
        matches!(entry.status, PlanStatus::Blocked(_))
            || entry.status == PlanStatus::Candidate && !reviewed.contains(&entry.request.item_id)
    }) {
        return Err(ManagementError::Confirmation);
    }
    if review.scope() != auth.database_scope
        || review.identity() != auth.database_identity
        || review.principal() != auth.principal
    {
        return Err(ManagementError::Unauthorized);
    }
    let keys = rows
        .iter()
        .map(|row| source.budget()?.key(row))
        .collect::<Result<Vec<_>>>()?;
    let now = crate::store::database_clock::database_now(&tx)
        .await
        .map_err(|_| ManagementError::Database)?;
    if now < review.staged_at() || now >= review.expires_at() {
        return Err(ManagementError::Expired);
    }
    let first_binding = native::value(first, "binding").ok_or(ManagementError::InvalidEvidence)?;
    let already_confirmed = !evidence::is_draft(first_binding, &mut *source.budget()?)?;
    if already_confirmed && review.candidate_item_ids() != Some(reviewed) {
        return Err(ManagementError::Confirmation);
    }
    let encoded = review.confirmation_encoding(now, reviewed)?;
    let shared = archive::shared_binding(&encoded);
    if !already_confirmed {
        for row in &rows {
            let raw = native::value(row, "binding").ok_or(ManagementError::InvalidEvidence)?;
            if !evidence::is_draft(raw, &mut *source.budget()?)?
                || raw != first_binding
                || native::value(row, "state") != Some("prepared")
            {
                return Err(ManagementError::Confirmation);
            }
            source.validate_draft(row)?;
        }
        let mut fresh = management::db_snapshot::collect_in_transaction_with_session(
            &tx,
            config,
            review.request(),
            work.clone(),
        )
        .await
        .map_err(snapshot_error)?
        .planning_snapshot()
        .map_err(|_| ManagementError::Changed)?;
        let frame = fresh
            .native_frame
            .take()
            .ok_or(ManagementError::Changed)?
            .without_records(&keys);
        for domain in &mut fresh.ledger_domains {
            domain.frontier = frame.frontier.clone();
        }
        for inventory in &mut fresh.inventories {
            inventory.frontier = frame.frontier.clone();
        }
        fresh.native_frame = Some(frame);
        management::plan::revalidate_review(review.view(now, reviewed), &fresh).map_err(
            |error| {
                if error == management::SafeError::LimitExceeded {
                    ManagementError::Budget
                } else {
                    ManagementError::Changed
                }
            },
        )?;
        for key in &keys {
            let old = native::value(source.row(key)?, "binding")
                .ok_or(ManagementError::InvalidEvidence)?;
            let item_binding = if native::value(source.row(key)?, "native_before")
                .is_some_and(|raw| raw.starts_with(archive::FRAME))
            {
                &encoded
            } else {
                &shared
            };
            let changed=tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),"UPDATE pin_management_records SET binding=$1 WHERE database_scope=$2 AND plan_id=$3 AND item_id=$4 AND kind='action' AND state='prepared' AND binding=$5",[item_binding.clone().into(),auth.database_scope.clone().into(),review.request().plan_id.clone().into(),key.values[2].clone().into(),old.into()])).await.map_err(|_|ManagementError::Database)?;
            if changed.rows_affected() != 1 {
                return Err(ManagementError::Confirmation);
            }
        }
        seal_originals(&tx, &keys, DecodeWork::shared(&work)).await?;
    }
    tx.commit().await.map_err(|_| ManagementError::Database)?;
    let source = evidence::Evidence::load_with_work(
        db,
        &auth.database_scope,
        &auth.database_identity,
        std::slice::from_ref(&request.plan_id),
        work,
    )
    .await?;
    let first = source
        .archive_members(&auth.database_scope, &request.plan_id)
        .next()
        .ok_or(ManagementError::Confirmation)?;
    decode_binding_from_row(&source, first)
}

pub(super) async fn stored_binding_with_work<C: ConnectionTrait>(
    db: &C,
    auth: &LocalAuthorization,
    binding: &ConfirmedBinding,
    work: SharedDecodeBudget,
) -> Result<Vec<native::NativeKey>> {
    let source = evidence::Evidence::load_with_work(
        db,
        &auth.database_scope,
        &auth.database_identity,
        std::slice::from_ref(&binding.request.plan_id),
        work,
    )
    .await?;
    let root = source
        .archive_members(&auth.database_scope, &binding.request.plan_id)
        .next()
        .ok_or(ManagementError::Confirmation)?;
    let confirmed = source.read_binding(root)?;
    if confirmed.view() != binding {
        return Err(ManagementError::Confirmation);
    }
    let mut keys = Vec::new();
    for row in source.archive_members(&auth.database_scope, &binding.request.plan_id) {
        keys.push(source.budget()?.key(row)?);
    }
    Ok(keys)
}

/// Fresh pure semantics and exact original fences in B's transaction. The
/// caller, not this function, owns publication locks/CAS and retirement admission.
pub async fn revalidate_confirmed_in_transaction(
    tx: &DatabaseTransaction,
    config: &ManagementConfig,
    auth: &LocalAuthorization,
    binding: &ConfirmedBinding,
) -> Result<Vec<management::CheckedItem>> {
    revalidate_with_work(tx, config, auth, binding, SharedDecodeBudget::default()).await
}
pub(crate) async fn revalidate_with_work(
    tx: &DatabaseTransaction,
    config: &ManagementConfig,
    auth: &LocalAuthorization,
    binding: &ConfirmedBinding,
    work: SharedDecodeBudget,
) -> Result<Vec<management::CheckedItem>> {
    revalidate_scoped_with_work(tx, config, auth, binding, None, work).await
}

pub(crate) async fn revalidate_scoped_with_work(
    tx: &DatabaseTransaction,
    config: &ManagementConfig,
    auth: &LocalAuthorization,
    binding: &ConfirmedBinding,
    scope: Option<&management::db_snapshot::PlanningScope>,
    work: SharedDecodeBudget,
) -> Result<Vec<management::CheckedItem>> {
    binding.check_deadline(tx, auth).await?;
    let source = evidence::Evidence::load_with_work(
        tx,
        &auth.database_scope,
        &auth.database_identity,
        std::slice::from_ref(&binding.request.plan_id),
        work.clone(),
    )
    .await?;
    let root = source
        .archive_members(&auth.database_scope, &binding.request.plan_id)
        .next()
        .ok_or(ManagementError::Confirmation)?;
    let confirmed = source.read_binding(root)?;
    if confirmed.view() != binding {
        return Err(ManagementError::Confirmation);
    }
    inventory_review::hydrate(tx, &confirmed.review(), &work).await?;
    inventory_review::seal_confirmed(tx, binding).await?;
    let keys = source
        .archive_members(&auth.database_scope, &binding.request.plan_id)
        .map(|row| source.budget()?.key(row))
        .collect::<Result<Vec<_>>>()?;
    let mut current = if let Some(scope) = scope {
        let now = crate::store::database_clock::database_now(tx)
            .await
            .map_err(|_| ManagementError::Database)?;
        scope
            .planning_snapshot(config, &binding.request, now, &work)
            .map_err(snapshot_error)?
    } else {
        management::db_snapshot::collect_in_transaction_with_session(
            tx,
            config,
            &binding.request,
            work,
        )
        .await
        .map_err(snapshot_error)?
        .planning_snapshot()
        .map_err(|_| ManagementError::Changed)?
    };
    let frame = current
        .native_frame
        .take()
        .ok_or(ManagementError::Changed)?
        .without_records(&keys);
    for domain in &mut current.ledger_domains {
        domain.frontier = frame.frontier.clone();
    }
    for inventory in &mut current.inventories {
        inventory.frontier = frame.frontier.clone();
    }
    current.native_frame = Some(frame);
    management::plan::revalidate_review(confirmed.view(), &current).map_err(|error| {
        if error == management::SafeError::LimitExceeded {
            ManagementError::Budget
        } else {
            ManagementError::Changed
        }
    })
}

/// Expired plans can still report a durable applied result. The original binding
/// is restored internally without granting publication or a renewed deadline.
pub async fn lookup_applied_from_imported<C: ConnectionTrait>(
    db: &C,
    auth: &LocalAuthorization,
    imported: &ImportedPlan,
    item_id: &str,
) -> Result<Option<ActionReceipt>> {
    auth.check(db).await?;
    if !imported.untrusted_applied_claims().is_empty() {
        return Err(ManagementError::Confirmation);
    }
    let source = evidence::Evidence::load_with_work(
        db,
        &auth.database_scope,
        &auth.database_identity,
        &[imported.request().plan_id.clone()],
        imported.session(),
    )
    .await?;
    let Some(row) = source
        .archive_members(&auth.database_scope, &imported.request().plan_id)
        .find(|row| native::value(row, "item_id") == Some(item_id))
    else {
        return Ok(None);
    };
    if native::value(row, "state") != Some("applied") {
        return Ok(None);
    }
    let stored = source.read_binding(row)?;
    if stored.principal != auth.principal
        || stored.database_scope != auth.database_scope
        || stored.database_identity != auth.database_identity
    {
        return Err(ManagementError::Unauthorized);
    }
    super::imported::validate_reviewed(&stored.review(), imported)?;
    results::trusted_from_record(&source, &auth.database_scope, &auth.database_identity, row)?
        .map(|trusted| {
            let receipt = &trusted.action_view().receipt;
            trusted.binding().copy(receipt)?;
            Ok(receipt.clone())
        })
        .transpose()
}
