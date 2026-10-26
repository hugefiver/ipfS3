//! Durable typed record seams for B/C. No business action is implemented here.
use super::{
    ManagementError, Result,
    native::{self, NativeFrame},
};
use crate::pinning::management::PlanRequest;
use chrono::{DateTime, Utc};
use sea_orm::{ConnectionTrait, DatabaseTransaction, Statement};
use serde::{Deserialize, Serialize};
mod archive;
pub(crate) use archive::binding_matches;
mod authority;
mod recorded;
pub use authority::{CheckpointEvidence, load_checkpoint_confirmation, load_confirmed_authority};
pub use recorded::{RecordedPlan, RecordedResult, load_recorded_plan};
mod codec;
mod confirmation;
mod current;
pub(crate) mod evidence;
mod imported;
mod index;
mod inspection;
mod inventory_review;
pub mod locator;
mod results;
mod retirement;
pub(crate) mod roots;
mod row;
mod session;
mod validation;
mod views;
pub use inventory_review::bind_sealed_retirement_review;
pub use inventory_review::copy_original_inventory;
pub(crate) use inventory_review::finish_apply as finish_retirement_apply;
pub use inventory_review::load_original_inventory;
pub(crate) use inventory_review::prepare_apply as prepare_retirement_apply;
pub(crate) use inventory_review::validate_entry as validate_retirement_entry;
pub(crate) use inventory_review::{OriginalInventoryDocument, open_original_inventory_document};
pub mod segmented;
pub(crate) use archive::ArchivedRows;
pub use archive::ConfirmedPlan;
pub use archive::ConfirmedPlan as ConfirmedBinding;
pub use archive::ReviewHandle;
pub(crate) use current::{current_management_rows, current_model, current_models};
pub(crate) use index::bounded_projection;
pub(crate) use inventory_review::prepared_output_reference;
pub(crate) use inventory_review::stage_canonical_inventory;
pub(crate) use inventory_review::stage_canonical_output;
pub(crate) use row::{ExecutionRows, FrozenRows, RowHandle, RowView};

pub(crate) async fn current_row<C: ConnectionTrait>(
    db: &C,
    name: &str,
    key: &[&str],
    work: DecodeWork<'_>,
) -> Result<native::NativeRow> {
    let table = native::current_catalog()
        .iter()
        .find(|table| table.table == name)
        .ok_or(ManagementError::Schema)?;
    if table.primary_key.len() != key.len() {
        return Err(ManagementError::Confirmation);
    }
    work.copy(&key)?;
    let predicate = table
        .primary_key
        .iter()
        .enumerate()
        .map(|(i, column)| format!("\"{column}\"=${}", i + 1))
        .collect::<Vec<_>>()
        .join(" AND ");
    work.copy(&table.columns)?;
    let columns = table
        .columns
        .iter()
        .map(|column| (column.as_str(), column.as_str()))
        .collect::<Vec<_>>();
    let (rows, bytes) = index::bounded_projection(
        db,
        &format!("FROM \"{name}\" WHERE {predicate}"),
        &columns,
        &predicate,
        table
            .primary_key
            .iter()
            .zip(key)
            .map(|(column, value)| native::scope::bound_value(table, column, value))
            .collect::<Result<Vec<_>>>()?,
        (1, native::MAX_BYTES),
        work,
    )
    .await?;
    work.index_cells(table.columns.len(), bytes)?;
    work.copy(&table.columns)?;
    let row = rows.first().ok_or(ManagementError::Changed)?;
    table
        .columns
        .iter()
        .map(|column| {
            Ok((
                column.clone(),
                row.try_get::<Option<String>>("", column)
                    .map_err(|_| ManagementError::InvalidEvidence)?,
            ))
        })
        .collect()
}

pub(crate) async fn read_confirmed_plan<C: ConnectionTrait>(
    db: &C,
    binding: &ConfirmedBinding,
    work: SharedDecodeBudget,
) -> Result<ConfirmedPlan> {
    let source = evidence::Evidence::load_with_work(
        db,
        &binding.database_scope,
        &binding.database_identity,
        std::slice::from_ref(&binding.request.plan_id),
        work,
    )
    .await?;
    let key = evidence::anchor_key(binding)?;
    let confirmed = source.read_binding(source.row(&key)?)?;
    if confirmed.view() != binding {
        return Err(ManagementError::Confirmation);
    }
    Ok(confirmed)
}

pub(crate) fn confirmed_members(
    binding: &ConfirmedBinding,
    work: SharedDecodeBudget,
) -> Result<Vec<std::sync::Arc<native::NativeRow>>> {
    let mut work = work.lock().map_err(|_| ManagementError::InvalidEvidence)?;
    let candidates =
        archive::sql_candidates(&binding.database_scope, &binding.request.plan_id, &mut work)?;
    work.nodes(1 + binding.request.items.len())?;
    let mut members = Vec::new();
    for entry in binding.confirmation.bindings.iter() {
        let mut matching = candidates.iter().filter(|row| {
            native::value(row, "item_id") == Some(&entry.request.item_id)
                && native::value(row, "kind") == Some("action")
                && native::value(row, "state") == Some("prepared")
                && native::value(row, "result").is_none()
                && native::value(row, "binding")
                    .zip(binding.original_binding().ok())
                    .is_some_and(|(left, right)| archive::binding_matches(left, right))
        });
        let row = matching.next().ok_or(ManagementError::Confirmation)?;
        if matching.any(|other| other.as_ref() != row.as_ref()) {
            return Err(ManagementError::Confirmation);
        }
        members.push(row.clone());
    }
    Ok(members)
}
pub(crate) use evidence::Budget as DecodeBudget;
pub use session::ReadSession;
pub(crate) use validation::ValidationContext;
pub use views::SharedDecodeBudget;
pub(crate) use views::{BindingView, DecodeWork};
#[cfg(test)]
pub(crate) mod fault_injection;
pub(crate) use confirmation::revalidate_scoped_with_work;
pub use confirmation::{
    PlanLocator, ReadyPublication, confirm_imported, confirm_ready, lookup_applied_from_imported,
    revalidate_confirmed_in_transaction, stage_confirmation,
};
pub use imported::{load_confirmed_from_imported, load_review_from_imported};
pub(crate) use index::action_at_key_with_work;
pub(crate) use index::actions_at_keys_catalog;
pub(crate) use index::actions_at_keys_with_work;
pub(crate) use index::{actions_for_plan_catalog, actions_for_plan_with_work};
pub(crate) use inspection::{RecordPayload, inspect_original_row, inspect_original_row_with_work};
pub(crate) use inspection::{inspect_retirement_review_with_work, inspect_review_with_work};
#[cfg(test)]
pub(crate) use results::decode_result as decode_action_result;
#[cfg(test)]
pub(crate) use results::frame_actions;
pub(crate) use results::frame_actions_with_work;
pub(crate) use results::frozen_actions_for_leases;
pub(crate) use results::lookup_trusted_with_work;
#[cfg(test)]
pub(crate) use results::private_action;
pub use results::{
    ActionState, CommittedAction, JobAuthority, JobDirective, MigrationPoint,
    NamespacePlanningProjection, OperatorDecision, lookup_committed,
};
pub(crate) use results::{
    TrustedAppliedAction, lookup_trusted_committed, write_executed_action_in_transaction,
};
pub(crate) use results::{
    frozen_actions_for_lease, private_action_from_rows, private_action_from_rows_with_work,
};
pub(crate) use retirement::classify_retirement_root;
pub(crate) use retirement::retirement_review_from_source;
pub(crate) use retirement::write_retirement_marker;
pub use retirement::{
    RetirementState, read_retirement, read_retirement_with_session, write_retirement_in_transaction,
};

/// Constructed only after local Config validation and native DB identity checks.
#[derive(Clone)]
pub struct LocalAuthorization {
    principal: String,
    database_scope: String,
    database_identity: String,
}
impl LocalAuthorization {
    pub fn principal(&self) -> &str {
        &self.principal
    }
    pub fn database_scope(&self) -> &str {
        &self.database_scope
    }
    pub fn database_identity(&self) -> &str {
        &self.database_identity
    }
    pub async fn check<C: ConnectionTrait>(&self, db: &C) -> Result<()> {
        if self.database_identity.starts_with("diagnostic:") {
            return Err(ManagementError::Unauthorized);
        }
        if native::database_identity(db).await? != self.database_identity {
            return Err(ManagementError::Unauthorized);
        }
        let row = db
            .query_one(Statement::from_string(
                db.get_database_backend(),
                "SELECT database_scope FROM pin_management_records WHERE kind='database_scope'",
            ))
            .await
            .map_err(|_| ManagementError::Database)?
            .ok_or(ManagementError::Schema)?;
        if row
            .try_get::<String>("", "database_scope")
            .map_err(|_| ManagementError::Schema)?
            != self.database_scope
        {
            return Err(ManagementError::Unauthorized);
        }
        Ok(())
    }
}

pub async fn authorize_local<C: ConnectionTrait>(
    config: &crate::config::Config,
    db: &C,
) -> Result<LocalAuthorization> {
    config
        .validate_kubo()
        .map_err(|_| ManagementError::Unauthorized)?;
    if config.auth.credentials.is_empty()
        || config
            .auth
            .credentials
            .iter()
            .any(|c| c.access_key.is_empty() || c.secret_key.is_empty())
    {
        return Err(ManagementError::Unauthorized);
    }
    super::schema::validate(db).await?;
    super::catalog::validate(db, native::current_catalog()).await?;
    let (database_scope, database_identity) = native::scope::binding(db).await?;
    if database_identity.starts_with("diagnostic:") {
        return Err(ManagementError::Unauthorized);
    }
    Ok(LocalAuthorization {
        principal: "local_configuration".into(),
        database_scope,
        database_identity,
    })
}

impl ConfirmedBinding {
    pub async fn check<C: ConnectionTrait>(
        &self,
        db: &C,
        authorization: &LocalAuthorization,
    ) -> Result<()> {
        self.check_with_work(db, authorization, SharedDecodeBudget::default())
            .await
    }
    pub(crate) async fn check_with_work<C: ConnectionTrait>(
        &self,
        db: &C,
        authorization: &LocalAuthorization,
        work: SharedDecodeBudget,
    ) -> Result<()> {
        self.check_deadline(db, authorization).await?;
        confirmation::stored_binding_with_work(db, authorization, self, work).await?;
        Ok(())
    }
    pub(crate) async fn check_deadline<C: ConnectionTrait>(
        &self,
        db: &C,
        authorization: &LocalAuthorization,
    ) -> Result<()> {
        authorization.check(db).await?;
        if self.principal != authorization.principal
            || self.database_scope != authorization.database_scope
            || self.database_identity != authorization.database_identity
            || self.confirmation.plan_id != self.request.plan_id
        {
            return Err(ManagementError::Confirmation);
        }
        let frame = self.view().frozen_rows()?;
        if frame.identity() != self.database_identity || frame.scope() != self.database_scope {
            return Err(ManagementError::Confirmation);
        }
        let now = crate::store::database_clock::database_now(db)
            .await
            .map_err(|_| ManagementError::Database)?;
        if now < self.confirmed_at || now >= self.expires_at || self.expires_at <= self.confirmed_at
        {
            return Err(ManagementError::Expired);
        }
        Ok(())
    }
}

/// Convenience for an in-process plan. File workflows use the explicit stage
/// and confirm_imported functions; neither an ID nor wire data grants authority.
pub async fn confirm(
    db: &sea_orm::DatabaseConnection,
    config: &crate::pinning::management::local_config::ManagementConfig,
    authorization: &LocalAuthorization,
    proposal: (
        &PlanRequest,
        &crate::pinning::management::Snapshot,
        &crate::pinning::management::DryRunPlan,
    ),
    candidate_item_ids: Vec<String>,
    ttl: chrono::Duration,
) -> Result<ConfirmedBinding> {
    confirm_with_session(
        db,
        config,
        authorization,
        proposal,
        &candidate_item_ids,
        ttl,
        SharedDecodeBudget::default(),
    )
    .await
}
pub async fn confirm_with_session(
    db: &sea_orm::DatabaseConnection,
    config: &crate::pinning::management::local_config::ManagementConfig,
    authorization: &LocalAuthorization,
    proposal: (
        &PlanRequest,
        &crate::pinning::management::Snapshot,
        &crate::pinning::management::DryRunPlan,
    ),
    candidate_item_ids: &[String],
    ttl: chrono::Duration,
    work: SharedDecodeBudget,
) -> Result<ConfirmedPlan> {
    let (request, _, plan) = proposal;
    let acknowledged = crate::pinning::management::plan::validate_acks(plan, candidate_item_ids)
        .map_err(|_| ManagementError::Confirmation)?;
    if plan.items().iter().any(|entry| {
        matches!(
            entry.status,
            crate::pinning::management::PlanStatus::Blocked(_)
        ) || entry.status == crate::pinning::management::PlanStatus::Candidate
            && !acknowledged.contains(&entry.request.item_id)
    }) {
        return Err(ManagementError::Confirmation);
    }
    if request.items.iter().any(|item| {
        matches!(
            item.subject,
            crate::pinning::management::Subject::Provider { .. }
        )
    }) {
        return confirmation::confirm_global(
            db,
            config,
            authorization,
            proposal,
            candidate_item_ids,
            ttl,
            work,
        )
        .await;
    }
    confirmation::stage_with_work(db, config, authorization, proposal, ttl, work.clone()).await?;
    confirmation::confirm_staged(
        db,
        config,
        authorization,
        request,
        candidate_item_ids,
        None,
        work,
    )
    .await
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionReceipt {
    pub binding: crate::pinning::management::PlanEntry,
    pub intent_ids: Vec<String>,
    pub job_ids: Vec<String>,
}

fn decode_binding_from_row(
    source: &evidence::Evidence,
    row: &native::NativeRow,
) -> Result<ConfirmedBinding> {
    source.binding(row, &mut *source.budget()?)
}

async fn write_result_with_work(
    tx: &DatabaseTransaction,
    auth: &LocalAuthorization,
    confirmation: &ConfirmedBinding,
    receipt: &ActionReceipt,
    result: &str,
    work: SharedDecodeBudget,
) -> Result<DateTime<Utc>> {
    // B owns native locking/recheck and the preceding business mutation. This
    // seam records its result in that SAME transaction, not an apply operation.
    let now = crate::store::database_clock::database_now(tx)
        .await
        .map_err(|_| ManagementError::Database)?;
    let candidates = {
        let mut budget = work.lock().map_err(|_| ManagementError::InvalidEvidence)?;
        archive::sql_candidates(
            &auth.database_scope,
            &confirmation.request.plan_id,
            &mut budget,
        )?
    };
    let row = candidates
        .iter()
        .find(|row| {
            native::value(row, "item_id") == Some(receipt.binding.request.item_id.as_str())
                && native::value(row, "kind") == Some("action")
        })
        .ok_or(ManagementError::Confirmation)?;
    let encoded = native::value(row, "binding").ok_or(ManagementError::Confirmation)?;
    if !archive::binding_matches(encoded, confirmation.original_binding()?) {
        return Err(ManagementError::Confirmation);
    }
    DecodeWork::shared(&work).copy(encoded)?;
    DecodeWork::shared(&work).copy(&(
        result,
        &receipt.intent_ids,
        &receipt.job_ids,
        &auth.database_scope,
        &confirmation.request.plan_id,
        &receipt.binding.request.item_id,
    ))?;
    #[cfg(test)]
    fault_injection::before_write(tx, auth, confirmation, receipt, &work).await?;
    let changed=tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),
        "UPDATE pin_management_records SET result=$1,intent_ids=$2,job_ids=$3,state='applied',completed_at=$4 WHERE database_scope=$5 AND plan_id=$6 AND item_id=$7 AND kind='action' AND state='prepared' AND result IS NULL AND binding=$8",
        [result.to_owned().into(),serde_json::to_string(&receipt.intent_ids).map_err(|_|ManagementError::InvalidEvidence)?.into(),serde_json::to_string(&receipt.job_ids).map_err(|_|ManagementError::InvalidEvidence)?.into(),now.into(),auth.database_scope.clone().into(),confirmation.request.plan_id.clone().into(),receipt.binding.request.item_id.clone().into(),encoded.into()])).await.map_err(|_|ManagementError::Database)?;
    if changed.rows_affected() != 1 {
        return Err(ManagementError::Confirmation);
    }
    Ok(now)
}

pub async fn lookup_applied<C: ConnectionTrait>(
    db: &C,
    authorization: &LocalAuthorization,
    confirmation: &ConfirmedBinding,
    item_id: &str,
) -> Result<Option<ActionReceipt>> {
    lookup_trusted_committed(db, authorization, confirmation, item_id)
        .await?
        .map(|trusted| {
            let receipt = &trusted.action_view().receipt;
            trusted.binding().copy(receipt)?;
            Ok(receipt.clone())
        })
        .transpose()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordKind {
    EvidencePreparation,
    Action,
}
impl RecordKind {
    fn sql(self) -> &'static str {
        match self {
            Self::EvidencePreparation => "evidence_preparation",
            Self::Action => "action",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordState {
    Prepared,
    Applied,
    Stopped,
    NeedsAttention,
}
impl RecordState {
    fn sql(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Applied => "applied",
            Self::Stopped => "stopped",
            Self::NeedsAttention => "needs_attention",
        }
    }
}

/// Complete binding and original before/result are retained per item, in the
/// caller's business transaction. A record ID alone never proves completion.
pub(crate) struct RecordWrite<'a> {
    pub plan_id: &'a str,
    pub item_id: &'a str,
    pub kind: RecordKind,
    pub binding: &'a str,
    pub native_before: &'a str,
    pub result: Option<&'a str>,
    pub intent_ids: &'a [String],
    pub job_ids: &'a [String],
    pub state: RecordState,
    pub expires_at: DateTime<Utc>,
}

pub(crate) async fn insert_in_transaction(
    tx: &DatabaseTransaction,
    auth: &LocalAuthorization,
    record: RecordWrite<'_>,
) -> Result<()> {
    auth.check(tx).await?;
    if record.plan_id.is_empty()
        || record.item_id.is_empty()
        || record
            .binding
            .len()
            .checked_add(record.native_before.len())
            .and_then(|n| n.checked_add(record.result.map_or(0, str::len)))
            .is_none_or(|n| n > 16 * 1024 * 1024)
    {
        return Err(ManagementError::Budget);
    }
    let now = crate::store::database_clock::database_now(tx)
        .await
        .map_err(|_| ManagementError::Database)?;
    if now >= record.expires_at {
        return Err(ManagementError::Expired);
    }
    let intent_ids =
        serde_json::to_string(record.intent_ids).map_err(|_| ManagementError::InvalidEvidence)?;
    let job_ids =
        serde_json::to_string(record.job_ids).map_err(|_| ManagementError::InvalidEvidence)?;
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),
        "INSERT INTO pin_management_records (database_scope,plan_id,item_id,kind,principal,native_identity,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
        [auth.database_scope.clone().into(),record.plan_id.into(),record.item_id.into(),record.kind.sql().into(),auth.principal.clone().into(),auth.database_identity.clone().into(),record.binding.into(),record.native_before.into(),record.result.map(str::to_owned).into(),intent_ids.into(),job_ids.into(),record.state.sql().into(),now.into(),record.expires_at.into(),record.result.map(|_| now).into()]))
        .await.map_err(|_| ManagementError::Database)?;
    if record.kind == RecordKind::EvidencePreparation {
        let key = native::NativeKey {
            table: super::schema::RECORDS.into(),
            values: vec![
                auth.database_scope.clone(),
                record.plan_id.into(),
                record.item_id.into(),
                record.kind.sql().into(),
            ],
        };
        seal_originals(
            tx,
            &[key],
            DecodeWork::shared(&SharedDecodeBudget::default()),
        )
        .await?;
    }
    Ok(())
}

/// Call only after the complete same-plan member set and bindings are written.
/// Action-result publication and route/legacy sources are sealed by their writers.
pub(crate) async fn seal_originals(
    tx: &DatabaseTransaction,
    keys: &[native::NativeKey],
    work: DecodeWork<'_>,
) -> Result<()> {
    let mut budget = native::meter::Budget::default();
    let result = async {
        for key in keys {
            locator::rebuild_source(tx, key, &mut budget).await?;
        }
        Ok(())
    }
    .await;
    charge_storage_work(&budget, work)?;
    result
}

fn charge_storage_work(budget: &native::meter::Budget, work: DecodeWork<'_>) -> Result<()> {
    work.with(|work| work.finish_storage(budget))
}

/// Exact native evidence guard; B must invoke this only after its lock/barrier
/// protocol and before publication. This does not acquire B's locks itself.
pub async fn recheck_frame<C: ConnectionTrait>(db: &C, original: &NativeFrame) -> Result<()> {
    native::scope::check_frame_scope(db, original, &mut native::meter::Budget::default()).await?;
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Disposition {
    Retain,
    Migrate,
    ControlledCleanup,
}

/// Persistent domain barrier shape; enforcement belongs to wave C.
#[derive(Clone, PartialEq, Eq)]
pub struct RetirementRecord {
    pub allocation_domain: String,
    pub database_scope: String,
    pub plan_id: String,
    pub item_id: String,
    pub principal: String,
    pub native_identity: String,
    pub binding: ConfirmedBinding,
    pub inventory: ReviewHandle,
    pub disposition: Disposition,
    pub unresolved: Vec<String>,
    pub state: RetirementState,
    pub created_at: DateTime<Utc>,
}
