//! Facts produced from actual SQL boundaries under the formal executor's locks.
use super::*;
use crate::pinning::management::{Action, Capture, PlanEntry};
use sea_orm::DatabaseTransaction;
#[cfg(test)]
use sea_orm::EntityTrait;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, sync::Arc};
mod allocation;
mod capture;
mod closure;
mod dependencies;
mod effects;
pub(crate) mod facts;
mod retirement;
mod rows;
mod validate;
use facts::{Birth, Cursor, ItemFacts, JobLink, OperationRead, Outcome, RowRef};
use rows::RowSet;

#[cfg(test)]
async fn publish_executed(
    auth: &records::LocalAuthorization,
    binding: &records::ConfirmedBinding,
    executed: &ExecutedAction<'_>,
    before: records::FrozenRows<'_>,
) -> Result<String> {
    let encoded =
        records::write_executed_action_in_transaction(auth, binding, executed, before).await?;
    super::seal::executed(
        executed.transaction(),
        auth,
        binding,
        executed,
        executed.work(),
    )
    .await?;
    Ok(encoded)
}

#[derive(PartialEq, Eq)]
pub(crate) struct Resource<'a> {
    pub(super) key: std::sync::Arc<native::NativeKey>,
    pub(super) remote: &'a records::RowHandle,
    pub(super) ledger: &'a records::RowHandle,
}
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum EntryOutcome<'a> {
    Ordinary,
    Extended {
        old_lease: &'a records::RowHandle,
        new_lease: &'a records::RowHandle,
        contract: Option<&'a str>,
        desired: Vec<&'a records::RowHandle>,
    },
    Kept {
        lease: &'a records::RowHandle,
        contract: Option<&'a str>,
        desired: Vec<&'a records::RowHandle>,
    },
}

/// An internal token retains the exact transaction and actual before/after rows;
/// the writer rechecks those rows, rather than trusting the token as a grant.
pub(crate) struct ExecutedAction<'tx> {
    tx: &'tx DatabaseTransaction,
    action: records::CommittedAction,
    operation: OperationRead,
    identity: String,
    work: records::SharedDecodeBudget,
    completed: std::sync::Mutex<Option<chrono::DateTime<chrono::Utc>>>,
    record_writes: std::sync::atomic::AtomicU64,
}
impl ExecutedAction<'_> {
    pub(crate) fn action_view(&self) -> &records::CommittedAction {
        &self.action
    }
    pub(super) fn into_action(self) -> records::CommittedAction {
        self.action
    }
    pub(crate) fn transaction(&self) -> &DatabaseTransaction {
        self.tx
    }
    pub(crate) fn operation(&self) -> &OperationRead {
        &self.operation
    }
    pub(crate) fn identity(&self) -> &str {
        &self.identity
    }
    pub(crate) fn work(&self) -> records::DecodeWork<'_> {
        records::DecodeWork::shared(&self.work)
    }
    pub(crate) fn published_at(&self, now: chrono::DateTime<chrono::Utc>) -> Result<()> {
        self.record_written();
        *self
            .completed
            .lock()
            .map_err(|_| ManagementError::Confirmation)? = Some(now);
        Ok(())
    }
    pub(crate) fn record_written(&self) {
        self.record_writes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    pub(crate) async fn validate_effect(&self, binding: records::BindingView<'_>) -> Result<()> {
        let binding = binding.with_work(&self.work);
        for output in &self.operation.facts.2 {
            if capture::row(self.tx, binding, &output.0)
                .await?
                .as_ref()
                .map(records::RowView::Native)
                != self
                    .operation
                    .after(&output.0)?
                    .map(records::RowView::Handle)
            {
                return Err(ManagementError::Confirmation);
            }
        }
        if facts::identity(binding, &self.action, &self.operation.facts)? != self.identity {
            return Err(ManagementError::Confirmation);
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn binding_for_test<'a>(
        &'a self,
        binding: &'a records::ConfirmedBinding,
    ) -> records::BindingView<'a> {
        records::BindingView::from(binding).with_work(&self.work)
    }
    #[cfg(test)]
    pub(crate) fn validate_job(
        &self,
        binding: &records::ConfirmedBinding,
        job: &records::JobAuthority,
    ) -> Result<()> {
        validate::job(
            self.binding_for_test(binding),
            &self.action,
            &self.operation,
            job,
        )
    }
}

pub(super) struct ExecutionBatch<'tx> {
    tx: &'tx DatabaseTransaction,
    config: &'tx ManagementConfig,
    auth: &'tx records::LocalAuthorization,
    binding: &'tx records::ConfirmedBinding,
    before: records::FrozenRows<'tx>,
    cursor: Cursor,
    ordinal: usize,
    work: records::SharedDecodeBudget,
    review: Option<records::ConfirmedPlan>,
    self_records: Vec<Arc<native::NativeRow>>,
    scope: Option<Arc<crate::pinning::management::db_snapshot::PlanningScope>>,
}
impl<'tx> ExecutionBatch<'tx> {
    #[cfg(test)]
    pub(super) fn new(
        tx: &'tx DatabaseTransaction,
        config: &'tx ManagementConfig,
        auth: &'tx records::LocalAuthorization,
        binding: &'tx records::ConfirmedBinding,
        before: records::FrozenRows<'tx>,
    ) -> Result<Self> {
        Self::with_work(
            tx,
            config,
            auth,
            binding,
            before,
            records::SharedDecodeBudget::default(),
        )
    }
    pub(super) fn with_work(
        tx: &'tx DatabaseTransaction,
        config: &'tx ManagementConfig,
        auth: &'tx records::LocalAuthorization,
        binding: &'tx records::ConfirmedBinding,
        before: records::FrozenRows<'tx>,
        work: records::SharedDecodeBudget,
    ) -> Result<Self> {
        let expected = records::BindingView::from(binding).frozen_rows()?;
        if !expected
            .all_rows()
            .map(|(table, row)| (&table.table, row))
            .eq(before.all_rows().map(|(table, row)| (&table.table, row)))
        {
            return Err(ManagementError::Confirmation);
        }
        Ok(Self {
            tx,
            config,
            auth,
            binding,
            before,
            cursor: Cursor::default(),
            ordinal: 0,
            work,
            review: None,
            self_records: Vec::new(),
            scope: None,
        })
    }
    pub(super) fn use_review(&mut self, review: records::ConfirmedPlan) -> Result<()> {
        if review.view() != self.binding {
            return Err(ManagementError::Confirmation);
        }
        self.review = Some(review);
        Ok(())
    }
    pub(super) fn use_scope(
        &mut self,
        scope: Arc<crate::pinning::management::db_snapshot::PlanningScope>,
    ) -> Result<()> {
        if scope.database_scope != self.binding.database_scope
            || scope.database_identity != self.binding.database_identity
        {
            return Err(ManagementError::Confirmation);
        }
        self.scope = Some(scope);
        Ok(())
    }
    pub(super) async fn execute_entry(&mut self, entry: &PlanEntry) -> Result<ExecutedAction<'tx>> {
        if self.binding.confirmation.bindings.get(self.ordinal) != Some(entry) {
            return Err(ManagementError::Confirmation);
        }
        if self.scope.is_none() {
            let scope =
                local_scope::capture(self.tx, self.config, self.binding, self.work.clone()).await?;
            self.use_scope(scope)?;
        }
        if self.review.is_none() {
            self.review =
                Some(records::read_confirmed_plan(self.tx, self.binding, self.work.clone()).await?);
        }
        let binding = self
            .review
            .as_ref()
            .ok_or(ManagementError::Confirmation)?
            .view()
            .with_work(&self.work);
        if self.self_records.is_empty() {
            self.self_records = records::confirmed_members(self.binding, self.work.clone())?;
            if self.self_records.len() != binding.confirmation.bindings.len() {
                return Err(ManagementError::Confirmation);
            }
        }
        let before = rows::LatestRows::before(binding, &self.cursor)?;
        let existing_count = before
            .iter()
            .filter(|(key, _)| key.table() == "pin_jobs")
            .count();
        binding.with_decode_work(|work| work.nodes(1 + existing_count))?;
        let existing = before
            .iter()
            .filter(|(key, _)| key.table() == "pin_jobs")
            .map(|(key, _)| key.value(0))
            .collect::<BTreeSet<_>>();
        // The locked dependency memberships and verified producer cursor supply
        // inputs; publication rechecks the complete original and produced scope,
        // including dependencies that remain unchanged.
        let boundary = if self.ordinal > 0 {
            binding.with_decode_work(|work| work.nodes(before.iter().count()))?;
            Some(records::ExecutionRows {
                scope: binding.database_scope,
                identity: binding.database_identity,
                rows: before
                    .iter()
                    .map(|(key, row)| {
                        native::current_catalog()
                            .iter()
                            .find(|table| table.table == key.table())
                            .map(|table| (table, row))
                            .ok_or(ManagementError::Schema)
                    })
                    .collect::<Result<Vec<_>>>()?,
            })
        } else {
            None
        };
        let (action, renewal) = executor::execute_with_jobs(
            self.tx,
            self.config,
            self.auth,
            entry,
            boundary
                .as_ref()
                .map_or(self.before, records::FrozenRows::Execution),
            &existing,
            binding,
        )
        .await?;
        let captured = capture::rows(self.tx, binding, &action, &before).await?;
        let after = rows::CapturedRows {
            before: &before,
            captured: &captured,
        };
        let mut keys = BTreeSet::new();
        closure::keys(binding, &action, &before, &after, &mut keys)?;
        let (reads, outputs) = capture::produce(binding, &before, &after, &self.cursor, &keys)?;
        let mut facts = ItemFacts(
            self.ordinal,
            reads,
            outputs,
            vec![],
            vec![],
            Outcome::Cancel(RowRef::Read(0), RowRef::Read(0)),
            vec![],
        );
        for key in captured
            .keys()
            .filter(|key| key.0 == "pin_jobs" && !before.contains_key(key))
        {
            let job = facts
                .2
                .iter()
                .position(|o| o.0 == *key)
                .ok_or(ManagementError::Confirmation)?;
            let invocation = facts
                .2
                .iter()
                .position(|o| o.0 == capture::key("pin_invocation_routes", &key.1[0]))
                .ok_or(ManagementError::Confirmation)?;
            facts.3.push(Birth(job, invocation));
        }
        facts
            .3
            .sort_by(|a, b| facts.2[a.0].0.1.cmp(&facts.2[b.0].0.1));
        for (ordinal, authority) in action.jobs.iter().enumerate() {
            let reference = |table| {
                let key = capture::key(table, &authority.job_id);
                if let Some(i) = facts.2.iter().position(|o| o.0 == key) {
                    Ok(RowRef::Output(i))
                } else {
                    facts
                        .1
                        .iter()
                        .position(|r| r.0 == key)
                        .map(RowRef::Read)
                        .ok_or(ManagementError::Confirmation)
                }
            };
            facts.4.push(JobLink(
                ordinal,
                reference("pin_jobs")?,
                reference("pin_invocation_routes")?,
            ));
        }
        facts.5 = closure::outcome(binding, &action, &facts, &before, &after, renewal)?;
        facts.6 = closure::dependencies(binding, &action)?;
        #[cfg(test)]
        if records::fault_injection::take_proof_fault(self.auth, self.binding, self.ordinal + 1) {
            facts.0 = usize::MAX;
        }
        binding.copy(&facts)?;
        let operation = OperationRead::resolve(binding, Arc::new(facts), &self.cursor)?;
        validate::closure(binding, &action, &operation, &self.cursor)?;
        let identity = facts::identity(binding, &action, &operation.facts)?;
        self.cursor.publish(&operation, &identity, binding)?;
        self.ordinal += 1;
        Ok(ExecutedAction {
            tx: self.tx,
            action,
            operation,
            identity,
            work: self.work.clone(),
            completed: std::sync::Mutex::new(None),
            record_writes: std::sync::atomic::AtomicU64::new(0),
        })
    }

    pub(super) async fn validate_final(
        &self,
        written: &[(ExecutedAction<'tx>, String)],
    ) -> Result<()> {
        let binding = self.binding.view().with_work(&self.work);
        let scope = self.scope.as_ref().ok_or(ManagementError::Confirmation)?;
        for key in scope.originals().keys() {
            binding.copy(key)?;
        }
        let mut expected = scope
            .originals()
            .iter()
            .map(|(key, row)| (key.clone(), std::borrow::Cow::Borrowed(row)))
            .collect::<local_scope::ExpectedRows<'_>>();
        let mut owned_sources = BTreeSet::new();
        for (executed, _) in written {
            for output in &executed.operation.facts.2 {
                let key = native::NativeKey {
                    table: output.0.0.clone(),
                    values: output.0.1.clone(),
                };
                if matches!(
                    key.table.as_str(),
                    "pin_provider_routes" | "pin_management_retirements"
                ) {
                    owned_sources.insert(key.clone());
                }
                if let Some(row) = executed.operation.after(&output.0)? {
                    let row = binding
                        .decode_work()
                        .copy_out(records::RowView::Handle(row))?;
                    expected.insert(key, std::borrow::Cow::Owned(row));
                } else {
                    expected.remove(&key);
                }
            }
        }
        let table = native::current_catalog()
            .iter()
            .find(|table| table.table == "pin_management_records")
            .ok_or(ManagementError::Schema)?;
        for original in &self.self_records {
            let (executed, encoded) = written
                .iter()
                .find(|(e, _)| {
                    Some(e.action.receipt.binding.request.item_id.as_str())
                        == native::value(original, "item_id")
                })
                .ok_or(ManagementError::Changed)?;
            let action = &executed.action;
            let key = native::NativeKey {
                table: table.table.clone(),
                values: table
                    .primary_key
                    .iter()
                    .map(|column| {
                        native::value(original.as_ref(), column)
                            .map(str::to_owned)
                            .ok_or(ManagementError::Confirmation)
                    })
                    .collect::<Result<_>>()?,
            };
            if scope.originals().get(&key) != Some(original.as_ref()) {
                return Err(ManagementError::Confirmation);
            }
            binding.copy(&(&action.receipt.intent_ids, &action.receipt.job_ids))?;
            let intents = serde_json::to_string(&action.receipt.intent_ids)
                .map_err(|_| ManagementError::Confirmation)?;
            let jobs = serde_json::to_string(&action.receipt.job_ids)
                .map_err(|_| ManagementError::Confirmation)?;
            let cells = original
                .iter()
                .filter(|(c, _)| c.as_str() != "completed_at")
                .map(|(column, value)| {
                    (
                        column.as_str(),
                        match column.as_str() {
                            "result" => Some(encoded.as_str()),
                            "intent_ids" => Some(intents.as_str()),
                            "job_ids" => Some(jobs.as_str()),
                            "state" => Some("applied"),
                            _ => value.as_deref(),
                        },
                    )
                });
            let completed = executed
                .completed
                .lock()
                .map_err(|_| ManagementError::Confirmation)?
                .ok_or(ManagementError::Confirmation)?;
            capture::matches(self.tx, binding, table, cells, Some(completed)).await?;
            let current = capture::row(
                self.tx,
                binding,
                &facts::Key(key.table.clone(), key.values.clone()),
            )
            .await?
            .ok_or(ManagementError::Changed)?;
            expected.insert(key.clone(), std::borrow::Cow::Owned(current));
            owned_sources.insert(key);
        }
        if self.self_records.len() != written.len() {
            return Err(ManagementError::Changed);
        }
        local_scope::validate_final(
            self.tx,
            self.config,
            self.binding,
            scope,
            &expected,
            &owned_sources,
            self.work.clone(),
        )
        .await?;
        if self
            .binding
            .confirmation
            .bindings
            .iter()
            .any(|entry| matches!(entry.capture, Some(Capture::Provider(_))))
        {
            let mut counts = std::collections::BTreeMap::<native::NativeKey, u64>::new();
            for (executed, _) in written {
                for output in &executed.operation.facts.2 {
                    *counts
                        .entry(native::NativeKey {
                            table: output.0.0.clone(),
                            values: output.0.1.clone(),
                        })
                        .or_default() += 1;
                }
                counts.insert(
                    native::NativeKey {
                        table: "pin_management_records".into(),
                        values: vec![
                            self.binding.database_scope.clone(),
                            self.binding.request.plan_id.clone(),
                            executed.action.receipt.binding.request.item_id.clone(),
                            "action".into(),
                        ],
                    },
                    executed
                        .record_writes
                        .load(std::sync::atomic::Ordering::Relaxed),
                );
            }
            let changes = counts
                .into_iter()
                .map(
                    |(key, writes)| crate::pinning::management::retirement::SourceTransition {
                        before: scope.originals().get(&key).cloned(),
                        after: expected.get(&key).map(|row| row.as_ref().clone()),
                        key,
                        writes,
                    },
                )
                .collect();
            records::finish_retirement_apply(self.tx, self.binding, changes).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn validate_operation(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    operation: &OperationRead,
) -> Result<()> {
    validate::action(binding, action, operation)
}
pub(super) use dependencies::destination_legacy;
pub(in crate::store::pinning::management::actions) use retirement::validate_job as validate_retirement_job;
pub(crate) fn validate_operation_with_cursor(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    operation: &OperationRead,
    cursor: &Cursor,
) -> Result<()> {
    validate::closure(binding, action, operation, cursor)
}
pub(crate) fn validate_job(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    operation: &OperationRead,
    job: &records::JobAuthority,
) -> Result<()> {
    validate::job(binding, action, operation, job)
}
pub(super) fn cell<'a>(row: impl native::RowValue<'a>, column: &str) -> Result<&'a str> {
    native::value(row, column).ok_or(ManagementError::Confirmation)
}
pub(super) fn epoch(row: &impl native::RawCells) -> Result<i64> {
    cell(row, "epoch")?
        .parse()
        .map_err(|_| ManagementError::Confirmation)
}
pub(super) fn desired(row: &impl native::RawCells) -> bool {
    native::value(row, "state")
        .is_some_and(|state| matches!(state, "waiting" | "submitted" | "pinned" | "degraded"))
}

#[cfg(test)]
mod facts_tests;
#[cfg(test)]
pub(crate) mod proof_tests;
#[cfg(test)]
mod review_tests;
