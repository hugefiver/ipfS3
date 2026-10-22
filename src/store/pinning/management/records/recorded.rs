//! Locally verified historical replies. These values cannot authorize execution.
use super::*;
use crate::pinning::management;
use crate::store::initialization::history::{self, legacy};
mod integrity;

#[derive(Serialize)]
#[serde(untagged)]
pub enum RecordedResult {
    Current(CommittedAction),
    Historical(legacy::RecordedAction),
}

pub struct RecordedPlan {
    pub(crate) index: history::ReviewIndex,
    pub(crate) request: PlanRequest,
    pub(crate) entries: Vec<legacy::PlanEntry>,
    pub(crate) summary: management::Summary,
    pub(crate) snapshot: legacy::Snapshot,
    pub(crate) migration_points: Vec<MigrationPoint>,
    pub(crate) originals: Vec<(native::NativeKey, segmented::ArchiveRef)>,
    actions: Vec<RecordedResult>,
    integrity: integrity::Integrity,
}

impl RecordedPlan {
    pub fn actions(&self) -> &[RecordedResult] {
        &self.actions
    }

    /// Recheck fresh membership, seals and every original cell without parsing
    /// the immutable historical review into another execution representation.
    pub(crate) async fn check_current<C: ConnectionTrait>(
        &self,
        db: &C,
        auth: &LocalAuthorization,
        budget: &mut native::meter::Budget,
    ) -> Result<()> {
        if self.index.database_scope != auth.database_scope
            || self.index.database_identity != auth.database_identity
            || self.index.principal != auth.principal
        {
            return Err(ManagementError::Unauthorized);
        }
        auth.check(db).await?;
        self.integrity.check_current(db, auth, budget).await
    }
}

/// A converted, wholly applied plan is reply-only, including after expiry or
/// configuration drift. Partial/unexecuted conversions are never resumed here.
pub async fn load_recorded_plan<C: ConnectionTrait>(
    db: &C,
    auth: &LocalAuthorization,
    request: &PlanRequest,
    budget: &mut native::meter::Budget,
) -> Result<Option<RecordedPlan>> {
    auth.check(db).await?;
    management::validate_historical_request(request).map_err(|_| ManagementError::Confirmation)?;
    let key = |item: &str| native::NativeKey {
        table: super::super::schema::RECORDS.into(),
        values: vec![
            auth.database_scope.clone(),
            request.plan_id.clone(),
            item.into(),
            "action".into(),
        ],
    };
    let first = request.items.first().ok_or(ManagementError::Confirmation)?;
    let first_key = key(&first.item_id);
    let source_key = locator::source_key(&first_key)?;
    budget.charge(1, 1, 1, 0)?;
    let exists = db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT 1 AS present WHERE EXISTS(SELECT 1 FROM pin_management_source_control WHERE source_key=$1) OR EXISTS(SELECT 1 FROM pin_management_records WHERE database_scope=$2 AND plan_id=$3 AND item_id=$4 AND kind='action')",
        [source_key.into(), auth.database_scope.clone().into(), request.plan_id.clone().into(), first.item_id.clone().into()]
    )).await.map_err(|_| ManagementError::Database)?.is_some();
    if !exists {
        return Ok(None);
    }
    let Some(converted) = history::recover_source(db, &first_key, budget).await? else {
        return Ok(None);
    };
    let index = converted.review.ok_or(ManagementError::Confirmation)?;
    if index.database_scope != auth.database_scope
        || index.database_identity != auth.database_identity
        || index.principal != auth.principal
        || index.core_identity.is_none()
        || index.confirmed_at.is_none()
        || index.acknowledgements.is_none()
    {
        return Err(ManagementError::Confirmation);
    }
    let original_request: PlanRequest = history::read_part(db, &index.request, budget).await?;
    if original_request != *request {
        return Err(ManagementError::Confirmation);
    }
    let entries: Vec<legacy::PlanEntry> = history::read_part(db, &index.entries, budget).await?;
    if entries.len() != request.items.len() {
        return Err(ManagementError::Confirmation);
    }
    let mut actions = Vec::new();
    let mut originals = Vec::new();
    let mut integrity = integrity::Integrity::default();
    let mut decode = ReadSession::default();
    let mut seen = std::collections::BTreeSet::new();
    for entry in &entries {
        if !seen.insert(&entry.request.item_id)
            || !request.items.iter().any(|item| item == &entry.request)
        {
            return Err(ManagementError::Confirmation);
        }
        let key = key(&entry.request.item_id);
        let source = history::recover_source(db, &key, budget)
            .await?
            .ok_or(ManagementError::Confirmation)?;
        let review = source
            .review
            .as_ref()
            .ok_or(ManagementError::Confirmation)?;
        if review.core_identity != index.core_identity
            || review.confirmed_at != index.confirmed_at
            || review.staged_at != index.staged_at
            || review.expires_at != index.expires_at
            || review.acknowledgements != index.acknowledgements
        {
            return Err(ManagementError::Confirmation);
        }
        let original: native::NativeRow = history::read_part(db, &source.original, budget).await?;
        let original_binding =
            native::value(&original, "binding").ok_or(ManagementError::Confirmation)?;
        let action = if matches!(entry.capture, Some(legacy::Capture::Provider(_))) {
            let action = history::recover_recorded(db, &key, original_binding, budget)
                .await?
                .ok_or(ManagementError::Confirmation)?;
            if action.receipt.binding != *entry {
                return Err(ManagementError::Confirmation);
            }
            RecordedResult::Historical(action)
        } else {
            let action = history::recover_committed(db, &key, original_binding, budget)
                .await?
                .ok_or(ManagementError::Confirmation)?;
            if action.receipt.binding.request != entry.request
                || action.receipt.binding.id != entry.id
            {
                return Err(ManagementError::Confirmation);
            }
            if action.receipt.binding.status != entry.status
                || action.receipt.binding.step != entry.step
                || !match (&entry.capture, &action.receipt.binding.capture) {
                    (
                        Some(legacy::Capture::Object(before)),
                        Some(management::Capture::Object(current)),
                    ) => before == current,
                    (None, None) => true,
                    _ => false,
                }
            {
                return Err(ManagementError::Confirmation);
            }
            RecordedResult::Current(action)
        };
        actions.push(action);
        originals.push((key, source.original.clone()));
        integrity
            .remember(db, auth, source, original, budget, &mut decode)
            .await?;
    }
    integrity.complete(db, auth, budget, &mut decode).await?;
    let summary = history::read_part(db, &index.summary, budget).await?;
    let snapshot = history::read_part(db, &index.snapshot, budget).await?;
    let migration_points = history::read_part(db, &index.migration_points, budget).await?;
    let plan = RecordedPlan {
        index,
        request: original_request,
        entries,
        summary,
        snapshot,
        migration_points,
        originals,
        actions,
        integrity,
    };
    plan.check_current(db, auth, budget).await?;
    Ok(Some(plan))
}
