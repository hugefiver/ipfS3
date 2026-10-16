//! Strict business result envelope. The original foundation receipt remains
//! readable, but cannot masquerade as a management worker/point authority.
use super::*;
use crate::pinning::{
    identity::ProviderRouteSnapshot,
    management::{ExactObject, Intent},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionState {
    Stopped,
    Accepted,
    Queued,
    Waiting,
    ReadOnly,
    RemoteComplete,
    ReplicationComplete,
    NeedsAttention,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobDirective {
    Submit,
    ReadOnlyObserve,
    ExistingCleanup,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobAuthority {
    pub job_id: String,
    pub directive: JobDirective,
    pub provider: String,
    pub cid: String,
    pub remote_epoch: i64,
    pub route: ProviderRouteSnapshot,
    pub resource_key: native::NativeKey,
    pub resource_id: Option<String>,
    pub lease_id: Option<String>,
    pub lease_generation: Option<i64>,
    pub target_id: Option<String>,
    pub owner: Option<ExactObject>,
    pub source: Option<ExactObject>,
    pub lease_source: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub policy_id: Option<String>,
    pub provider_mode: Option<String>,
    pub content_mode: Option<String>,
    pub lease_contract: Option<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationPoint {
    pub table: String,
    pub primary_key: native::NativeKey,
    pub capture_position: String,
    pub original_contract: native::NativeRow,
    pub slot: String,
    pub lease_id: String,
    pub cid: String,
    pub source_target_id: String,
    pub source_allocation: String,
    pub source_route: ProviderRouteSnapshot,
    pub source_resource_route: ProviderRouteSnapshot,
    pub source_epoch: i64,
    pub source_resource_id: Option<String>,
    pub source_resource_key: native::NativeKey,
    pub source_generation: i64,
    pub source_lease: native::NativeRow,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predecessor: Option<native::NativeKey>,
    pub destination_allocation: String,
    pub destination_route: ProviderRouteSnapshot,
    pub destination_target_id: String,
    pub destination_epoch: i64,
    pub destination_resource_key: native::NativeKey,
    pub destination_resource_id: Option<String>,
    /// Destination satisfaction must still meet the original logical slot's
    /// complete threshold and freshness. No historical JSON is rewritten.
    pub started_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    /// Archived proposal data only. Formal stored points must keep this None.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planning_projection: Option<NamespacePlanningProjection>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespacePlanningProjection {
    pub legacy_record: native::NativeKey,
    pub allocation: String,
    pub execution_route: ProviderRouteSnapshot,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorDecision {
    pub id: String,
    pub owner: ExactObject,
    pub intent: Intent,
    pub stored_bytes: u64,
    pub configuration: crate::pinning::management::ConfigurationBinding,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommittedAction {
    pub receipt: ActionReceipt,
    pub state: ActionState,
    pub targets: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub root_releases: Vec<native::NativeKey>,
    pub jobs: Vec<JobAuthority>,
    pub migration_points: Vec<MigrationPoint>,
    pub decision: Option<OperatorDecision>,
}

use crate::store::pinning::management::actions::{
    execution::{
        self, ExecutedAction,
        facts::{Cursor, ItemFacts, OperationRead},
    },
    job_authority, points,
};
use std::sync::Arc;

pub(super) enum DecodedResult {
    Facts(CommittedAction, Arc<ItemFacts>, String),
}
enum Validation {
    Facts(Arc<OperationRead>, String),
}
pub(super) struct ValidatedPrefix {
    review: usize,
    cursor: Cursor,
    members: Vec<(Arc<native::NativeRow>, Arc<OperationRead>)>,
}

/// Created exclusively from an exact applied DB record or its exact hidden cells.
/// The data view cannot be converted back into this authority.
pub(crate) struct TrustedAppliedAction {
    work: std::sync::Arc<std::sync::Mutex<evidence::Budget>>,
    binding: evidence::ReadBinding,
    decoded: Arc<DecodedResult>,
    key: native::NativeKey,
    validation: Validation,
}
impl TrustedAppliedAction {
    pub(crate) fn binding(&self) -> BindingView<'_> {
        self.binding.view().with_work(&self.work)
    }
    pub(crate) fn action_view(&self) -> &CommittedAction {
        let DecodedResult::Facts(action, _, _) = self.decoded.as_ref();
        action
    }
    pub(crate) fn record_key(&self) -> &native::NativeKey {
        &self.key
    }
    pub(crate) fn validate_job(&self, job: &JobAuthority) -> Result<()> {
        if !self.action_view().jobs.contains(job) {
            return Err(ManagementError::Confirmation);
        }
        match &self.validation {
            Validation::Facts(operation, _) => {
                execution::validate_job(self.binding(), self.action_view(), operation, job)
            }
        }
    }
    pub(crate) fn validate_point(&self, point: &MigrationPoint) -> Result<()> {
        if !self.action_view().migration_points.contains(point) {
            return Err(ManagementError::Confirmation);
        }
        Ok(())
    }
    pub(crate) fn facts_identity(&self) -> Result<String> {
        match &self.validation {
            Validation::Facts(_, hash) => {
                self.binding().copy(hash)?;
                Ok(hash.clone())
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn decode_result(input: &str) -> Result<(ActionReceipt, Option<CommittedAction>)> {
    let DecodedResult::Facts(action, _, _) = decode_facts(
        input
            .strip_prefix("S7R1\n")
            .ok_or(ManagementError::InvalidEvidence)?,
        &mut DecodeBudget::default(),
    )?;
    Ok((action.receipt.clone(), Some(action)))
}
pub(super) fn result_from_row(
    source: &evidence::Evidence,
    row: &native::NativeRow,
) -> Result<(ActionReceipt, Option<CommittedAction>)> {
    let decoded = source.budget()?.result(row)?;
    let DecodedResult::Facts(action, _, _) = decoded.as_ref();
    source.budget()?.copy(&action.receipt)?;
    source.budget()?.copy(action)?;
    Ok((action.receipt.clone(), Some(action.clone())))
}

pub(super) fn decode_facts(body: &str, work: &mut DecodeBudget) -> Result<DecodedResult> {
    let (action, facts, hash): (CommittedAction, ItemFacts, String) = work.typed(body)?;
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ManagementError::InvalidEvidence);
    }
    work.nodes(1)?;
    Ok(DecodedResult::Facts(action, Arc::new(facts), hash))
}
fn encode_facts(
    action: &impl Serialize,
    facts: &ItemFacts,
    hash: &str,
    work: DecodeWork<'_>,
) -> Result<String> {
    let value = (action, facts, hash);
    let length = codec::measure(&value)?.1;
    work.with(|work| work.bytes(length + 5))?;
    let mut encoded = String::with_capacity(length + 5);
    encoded.push_str("S7R1\n");
    use std::fmt::Write;
    struct W<'a>(&'a mut String);
    impl std::io::Write for W<'_> {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0
                .write_str(std::str::from_utf8(b).map_err(std::io::Error::other)?)
                .map_err(std::io::Error::other)?;
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(W(&mut encoded), &value).map_err(|_| ManagementError::InvalidEvidence)?;
    Ok(encoded)
}

pub(crate) async fn write_executed_action_in_transaction(
    auth: &LocalAuthorization,
    binding: &ConfirmedBinding,
    executed: &ExecutedAction<'_>,
    before: FrozenRows<'_>,
) -> Result<String> {
    let tx = executed.transaction();
    let action = executed.action_view();
    binding.check_deadline(tx, auth).await?;
    if !BindingView::from(binding)
        .frozen_rows()?
        .all_rows()
        .map(|(table, row)| (&table.table, row))
        .eq(before.all_rows().map(|(table, row)| (&table.table, row)))
        || action
            .jobs
            .iter()
            .map(|job| job.job_id.clone())
            .collect::<Vec<_>>()
            != action.receipt.job_ids
    {
        return Err(ManagementError::Confirmation);
    }
    executed.validate_effect(binding.into()).await?;
    let view = binding.view().with_work(executed.work().budget());
    let rows = view.frozen_rows()?;
    roots::validate_rows(action, rows, view.decode_work())?;
    roots::validate_effect(tx, action, rows, view.decode_work()).await?;
    job_authority::validate_executed_source(tx, view, action, rows).await?;
    points::validate_birth(tx, view, action).await?;
    let encoded = encode_facts(
        action,
        &executed.operation().facts,
        executed.identity(),
        executed.work(),
    )?;
    let completed = write_result_with_work(
        tx,
        auth,
        binding,
        &action.receipt,
        &encoded,
        executed.work().budget().clone(),
    )
    .await?;
    executed.published_at(completed)?;
    Ok(encoded)
}

pub(crate) async fn update_executed_state(
    auth: &LocalAuthorization,
    binding: &ConfirmedPlan,
    executed: &ExecutedAction<'_>,
    original: &str,
    state: ActionState,
) -> Result<Option<String>> {
    let action = executed.action_view();
    if state == action.state {
        return Ok(None);
    }
    let work = executed.work();
    let updated = super::super::actions::execution::facts::with_state(action, state);
    let encoded = encode_facts(
        &updated,
        &executed.operation().facts,
        executed.identity(),
        work,
    )?;
    work.copy(&(
        original,
        &auth.database_scope,
        &binding.request.plan_id,
        &action.receipt.binding.request.item_id,
    ))?;
    let tx = executed.transaction();
    work.copy(&encoded)?;
    let changed=tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),"UPDATE pin_management_records SET result=$1 WHERE database_scope=$2 AND plan_id=$3 AND item_id=$4 AND kind='action' AND state='applied' AND result=$5",[encoded.clone().into(),auth.database_scope.clone().into(),binding.request.plan_id.clone().into(),action.receipt.binding.request.item_id.clone().into(),original.into()])).await.map_err(|_|ManagementError::Database)?;
    if changed.rows_affected() != 1 {
        return Err(ManagementError::Confirmation);
    }
    Ok(Some(encoded))
}

/// Static trust/binding validation only. Completion and worker consumers perform
/// their own current target/lease/epoch fences; a historical receipt is not live.
pub async fn lookup_committed<C: ConnectionTrait>(
    db: &C,
    auth: &LocalAuthorization,
    binding: &ConfirmedBinding,
    item: &str,
) -> Result<Option<CommittedAction>> {
    lookup_trusted_committed(db, auth, binding, item)
        .await?
        .map(|trusted| {
            trusted.binding().copy(trusted.action_view())?;
            Ok(trusted.action_view().clone())
        })
        .transpose()
}

pub(crate) async fn lookup_trusted_committed<C: ConnectionTrait>(
    db: &C,
    auth: &LocalAuthorization,
    binding: &ConfirmedBinding,
    item: &str,
) -> Result<Option<TrustedAppliedAction>> {
    lookup_trusted_with_work(db, auth, binding, item, SharedDecodeBudget::default()).await
}
pub(crate) async fn lookup_trusted_with_work<C: ConnectionTrait>(
    db: &C,
    auth: &LocalAuthorization,
    binding: &ConfirmedBinding,
    item: &str,
    work: SharedDecodeBudget,
) -> Result<Option<TrustedAppliedAction>> {
    auth.check(db).await?;
    if binding.principal != auth.principal
        || binding.database_scope != auth.database_scope
        || binding.database_identity != auth.database_identity
    {
        return Err(ManagementError::Unauthorized);
    }
    DecodeWork::shared(&work).copy(&(
        super::super::schema::RECORDS,
        &auth.database_scope,
        &binding.request.plan_id,
        item,
        "action",
    ))?;
    let key = native::NativeKey {
        table: super::super::schema::RECORDS.into(),
        values: vec![
            auth.database_scope.clone(),
            binding.request.plan_id.clone(),
            item.into(),
            "action".into(),
        ],
    };
    let Some(trusted) = index::action_at_key_with_work(db, &key, work).await? else {
        return Ok(None);
    };
    if trusted.binding() != binding {
        return Err(ManagementError::Confirmation);
    }
    Ok(Some(trusted))
}

/// Pure extraction from a complete native frame. Imported public frames lack
/// this original private DB material and cannot manufacture durable authority.
#[cfg(test)]
pub(crate) fn frame_actions(frame: &NativeFrame) -> Result<Vec<TrustedAppliedAction>> {
    frame_actions_with_work(frame, SharedDecodeBudget::default())
}
pub(crate) fn frame_actions_with_work(
    frame: &NativeFrame,
    work: SharedDecodeBudget,
) -> Result<Vec<TrustedAppliedAction>> {
    let rows = &frame
        .table(super::super::schema::RECORDS)
        .ok_or(ManagementError::Schema)?
        .rows;
    let mut actions = Vec::new();
    let mut context = ValidationContext::default();
    context.decoding = work;
    context
        .decoding
        .lock()
        .map_err(|_| ManagementError::InvalidEvidence)?
        .nodes(1)?;
    for row in rows.iter().filter(|row| {
        native::value(row, "kind") == Some("action")
            && native::value(row, "state") == Some("applied")
            && native::value(row, "completed_at").is_some()
    }) {
        let scope = native::value(row, "database_scope").ok_or(ManagementError::InvalidEvidence)?;
        let plan = native::value(row, "plan_id").ok_or(ManagementError::InvalidEvidence)?;
        let item = native::value(row, "item_id").ok_or(ManagementError::InvalidEvidence)?;
        if scope != frame.database_scope
            || native::value(row, "native_identity") != Some(&frame.database_identity)
            || native::value(row, "principal") != Some("local_configuration")
        {
            continue;
        }
        let key = [scope, plan, item, "action"];
        context
            .decoding
            .lock()
            .map_err(|_| ManagementError::InvalidEvidence)?
            .copy(&(super::super::schema::RECORDS, key))?;
        let key = native::NativeKey {
            table: super::super::schema::RECORDS.into(),
            values: key.iter().map(|value| (*value).to_owned()).collect(),
        };
        if let Some(action) = private_action_with_context(frame, &key, &mut context)? {
            actions.push(action);
        }
    }
    Ok(actions)
}

#[cfg(test)]
pub(crate) fn private_action(
    frame: &NativeFrame,
    key: &native::NativeKey,
) -> Result<Option<TrustedAppliedAction>> {
    private_action_with_context(frame, key, &mut ValidationContext::default())
}

pub(crate) fn private_action_with_context(
    frame: &NativeFrame,
    key: &native::NativeKey,
    context: &mut ValidationContext,
) -> Result<Option<TrustedAppliedAction>> {
    if frame.private_record_exact(key).is_none() {
        return Err(ManagementError::InvalidEvidence);
    }
    let source = evidence::Evidence::from_frame_with_work(frame, context.decoding.clone())?;
    let row = source.row(key)?;
    if ["binding", "native_before", "result"]
        .iter()
        .any(|column| native::value(row, column).is_none())
    {
        return Err(ManagementError::InvalidEvidence);
    }
    let trusted = trusted_from_record_with_context(
        &source,
        &frame.database_scope,
        &frame.database_identity,
        row,
        context,
    )?;
    if trusted
        .as_ref()
        .is_some_and(|trusted| trusted.record_key() != key)
    {
        return Err(ManagementError::Confirmation);
    }
    Ok(trusted)
}

pub(crate) fn private_action_from_rows(
    rows: FrozenRows<'_>,
    key: &native::NativeKey,
    context: &mut ValidationContext,
) -> Result<Option<TrustedAppliedAction>> {
    let source = evidence::Evidence::from_frozen_with_work(rows, context.decoding.clone())?;
    let row = source.row(key)?;
    let trusted =
        trusted_from_record_with_context(&source, rows.scope(), rows.identity(), row, context)?;
    if trusted
        .as_ref()
        .is_some_and(|trusted| trusted.record_key() != key)
    {
        return Err(ManagementError::Confirmation);
    }
    Ok(trusted)
}
pub(crate) fn private_action_from_rows_with_work(
    rows: FrozenRows<'_>,
    key: &native::NativeKey,
    work: DecodeWork<'_>,
) -> Result<Option<TrustedAppliedAction>> {
    private_action_from_rows(
        rows,
        key,
        &mut ValidationContext::with_work(work.budget().clone()),
    )
}
pub(crate) fn frozen_actions_for_lease(
    rows: FrozenRows<'_>,
    lease: &str,
    work: DecodeWork<'_>,
) -> Result<Vec<TrustedAppliedAction>> {
    frozen_actions_for_leases(rows, &[lease], work)
}
pub(crate) fn frozen_actions_for_leases(
    rows: FrozenRows<'_>,
    leases: &[&str],
    work: DecodeWork<'_>,
) -> Result<Vec<TrustedAppliedAction>> {
    let source = evidence::Evidence::from_frozen_with_work(rows, work.budget().clone())?;
    let mut context = ValidationContext::with_work(work.budget().clone());
    work.with(|work| work.nodes(1))?;
    let mut actions = Vec::new();
    for row in source.action_rows() {
        if native::value(row, "state") != Some("applied")
            || native::value(row, "database_scope") != Some(rows.scope())
            || native::value(row, "native_identity") != Some(rows.identity())
            || native::value(row, "principal") != Some("local_configuration")
        {
            continue;
        }
        let ids = work.with(|work| {
            work.typed_shared::<Vec<String>>(
                native::value(row, "intent_ids").ok_or(ManagementError::Confirmation)?,
            )
        })?;
        if !ids.iter().any(|id| leases.contains(&id.as_str())) {
            continue;
        }
        if let Some(action) = trusted_from_record_with_context(
            &source,
            rows.scope(),
            rows.identity(),
            row,
            &mut context,
        )? {
            work.with(|work| work.nodes(1))?;
            actions.push(action);
        }
    }
    Ok(actions)
}

// Only callers with real DB rows or exact private cells reach this constructor.
pub(super) fn trusted_from_record(
    source: &evidence::Evidence,
    scope: &str,
    identity: &str,
    row: &native::NativeRow,
) -> Result<Option<TrustedAppliedAction>> {
    let mut context = ValidationContext::with_work(source.work.clone());
    trusted_from_record_with_context(source, scope, identity, row, &mut context)
}

pub(super) fn trusted_from_record_with_context(
    source: &evidence::Evidence,
    scope: &str,
    identity: &str,
    row: &native::NativeRow,
    context: &mut ValidationContext,
) -> Result<Option<TrustedAppliedAction>> {
    if native::value(row, "kind") != Some("action")
        || native::value(row, "state") != Some("applied")
        || native::value(row, "completed_at").is_none()
    {
        return Ok(None);
    }
    if native::value(row, "database_scope") != Some(scope)
        || native::value(row, "native_identity") != Some(identity)
        || native::value(row, "principal") != Some("local_configuration")
    {
        return Err(ManagementError::Unauthorized);
    }
    let key = context
        .decoding
        .lock()
        .map_err(|_| ManagementError::InvalidEvidence)?
        .key(row)?;
    context.with_record(key, |context| {
        restore_record(source, scope, identity, row, context)
    })
}

fn restore_record(
    source: &evidence::Evidence,
    scope: &str,
    identity: &str,
    row: &native::NativeRow,
    context: &mut ValidationContext,
) -> Result<Option<TrustedAppliedAction>> {
    let owned_binding = source.read_binding(row)?;
    let work = context.decoding.clone();
    let binding = owned_binding.view().with_work(&work);
    let decoded = context
        .decoding
        .lock()
        .map_err(|_| ManagementError::InvalidEvidence)?
        .result(row)?;
    let DecodedResult::Facts(action, facts, hash) = decoded.as_ref();
    let receipt = &action.receipt;
    if binding.database_scope != scope
        || binding.database_identity != identity
        || binding.principal != "local_configuration"
        || native::value(row, "plan_id") != Some(&binding.request.plan_id)
        || native::value(row, "item_id") != Some(&receipt.binding.request.item_id)
        || !binding.confirmation.bindings.contains(&receipt.binding)
        || binding
            .decode_shared::<Vec<String>>(
                native::value(row, "intent_ids").ok_or(ManagementError::InvalidEvidence)?,
            )?
            .as_ref()
            != &receipt.intent_ids
        || binding
            .decode_shared::<Vec<String>>(
                native::value(row, "job_ids").ok_or(ManagementError::InvalidEvidence)?,
            )?
            .as_ref()
            != &receipt.job_ids
    {
        return Err(ManagementError::Confirmation);
    }
    roots::validate_rows(action, binding.frozen_rows()?, binding.decode_work())?;
    if action.state == ActionState::Stopped
        || !action
            .jobs
            .iter()
            .map(|job| &job.job_id)
            .eq(&receipt.job_ids)
        || action
            .jobs
            .iter()
            .map(|job| &job.job_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != action.jobs.len()
        || action
            .migration_points
            .iter()
            .map(|point| (&point.lease_id, &point.cid, &point.slot))
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != action.migration_points.len()
    {
        return Err(ManagementError::Confirmation);
    }
    let validation = {
        let cached = {
            let mut work = context
                .decoding
                .lock()
                .map_err(|_| ManagementError::InvalidEvidence)?;
            work.prefixes
                .iter()
                .position(|prefix| prefix.review == owned_binding.review_id())
                .map(|index| work.prefixes.swap_remove(index))
        };
        let cached = cached.filter(|prefix| {
            prefix
                .members
                .iter()
                .enumerate()
                .take(facts.0.saturating_add(1))
                .all(|(ordinal, (original, _))| {
                    binding
                        .confirmation
                        .bindings
                        .get(ordinal)
                        .and_then(|entry| {
                            source
                                .archive_members(scope, &binding.request.plan_id)
                                .find(|member| {
                                    native::value(member, "item_id") == Some(&entry.request.item_id)
                                })
                        })
                        .is_some_and(|member| member == original.as_ref())
                })
        });
        let mut prefix = if let Some(prefix) = cached {
            prefix
        } else {
            binding.with_decode_work(|work| work.nodes(2))?;
            ValidatedPrefix {
                review: owned_binding.review_id(),
                cursor: Cursor::default(),
                members: Vec::new(),
            }
        };
        let mut selected = None;
        for (ordinal, entry) in binding
            .confirmation
            .bindings
            .iter()
            .enumerate()
            .take(facts.0.saturating_add(1))
        {
            let member = source
                .archive_members(scope, &binding.request.plan_id)
                .find(|row| native::value(row, "item_id") == Some(&entry.request.item_id))
                .ok_or(ManagementError::Confirmation)?;
            if native::value(member, "state") != Some("applied") {
                return Err(ManagementError::Confirmation);
            }
            let decoded = context
                .decoding
                .lock()
                .map_err(|_| ManagementError::InvalidEvidence)?
                .result(member)?;
            let DecodedResult::Facts(producer, local, identity) = decoded.as_ref();
            if local.0 != ordinal
                || producer.receipt.binding != *entry
                || binding
                    .decode_shared::<Vec<String>>(
                        native::value(member, "intent_ids").ok_or(ManagementError::Confirmation)?,
                    )?
                    .as_ref()
                    != &producer.receipt.intent_ids
                || binding
                    .decode_shared::<Vec<String>>(
                        native::value(member, "job_ids").ok_or(ManagementError::Confirmation)?,
                    )?
                    .as_ref()
                    != &producer.receipt.job_ids
                || execution::facts::identity(binding, producer, local)? != *identity
            {
                return Err(ManagementError::Confirmation);
            }
            if let Some((_, operation)) = prefix.members.get(ordinal) {
                // Byte-identical coordinates are data; recursive authority and
                // its caller-local depth/path limits are checked on every use.
                for point in &producer.migration_points {
                    points::validate_original_with_context(binding, producer, point, context)?;
                }
                if ordinal == facts.0 {
                    selected = Some(operation.clone());
                }
                continue;
            }
            let operation = OperationRead::resolve(binding, local.clone(), &prefix.cursor)?;
            execution::validate_operation_with_cursor(
                binding,
                producer,
                &operation,
                &prefix.cursor,
            )?;
            for point in &producer.migration_points {
                points::validate_original_with_context(binding, producer, point, context)?;
            }
            prefix.cursor.publish(&operation, identity, binding)?;
            binding.with_decode_work(|work| work.nodes(3))?;
            let operation = Arc::new(operation);
            let original =
                binding.with_decode_work(|work| archive::retain_row_ref(member, work))?;
            prefix.members.push((original, operation.clone()));
            if ordinal == facts.0 {
                selected = Some(operation);
            }
        }
        if execution::facts::identity(binding, action, facts)? != *hash {
            return Err(ManagementError::Confirmation);
        }
        context
            .decoding
            .lock()
            .map_err(|_| ManagementError::InvalidEvidence)?
            .prefixes
            .push(prefix);
        binding.copy(hash)?;
        Validation::Facts(selected.ok_or(ManagementError::Confirmation)?, hash.clone())
    };
    let key = context
        .decoding
        .lock()
        .map_err(|_| ManagementError::InvalidEvidence)?
        .key(row)?;
    let trusted = TrustedAppliedAction {
        work: context.decoding.clone(),
        binding: owned_binding,
        decoded,
        key,
        validation,
    };
    for job in &trusted.action_view().jobs {
        trusted.validate_job(job)?;
    }
    context
        .decoding
        .lock()
        .map_err(|_| ManagementError::InvalidEvidence)?
        .check()?;
    Ok(Some(trusted))
}
