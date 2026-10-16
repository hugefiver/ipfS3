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
}
impl ExecutedAction<'_> {
    pub(crate) fn action_view(&self) -> &records::CommittedAction {
        &self.action
    }
    pub(super) fn set_state(&mut self, state: records::ActionState) {
        self.action.state = state;
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
        *self
            .completed
            .lock()
            .map_err(|_| ManagementError::Confirmation)? = Some(now);
        Ok(())
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
    pub(crate) fn validate_job(
        &self,
        binding: &records::ConfirmedBinding,
        job: &records::JobAuthority,
    ) -> Result<()> {
        validate::job(binding.into(), &self.action, &self.operation, job)
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
        })
    }
    pub(super) fn use_review(&mut self, review: records::ConfirmedPlan) -> Result<()> {
        if review.view() != self.binding {
            return Err(ManagementError::Confirmation);
        }
        self.review = Some(review);
        Ok(())
    }
    pub(super) async fn execute_entry(&mut self, entry: &PlanEntry) -> Result<ExecutedAction<'tx>> {
        if self.binding.confirmation.bindings.get(self.ordinal) != Some(entry) {
            return Err(ManagementError::Confirmation);
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
        // The locked registry fence and verified producer cursor supply inputs;
        // only semantic effects are captured and the full registry is fenced at
        // publication, including dependencies that remain unchanged.
        let (action, renewal) = executor::execute_with_jobs(
            self.tx,
            self.config,
            self.auth,
            entry,
            self.before,
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
        })
    }

    pub(super) async fn validate_final(
        &self,
        written: &[(ExecutedAction<'tx>, String)],
    ) -> Result<()> {
        let binding = self.binding.view().with_work(&self.work);
        let expected = rows::LatestRows::before(binding, &self.cursor)?;
        for table in native::current_catalog() {
            let mut count = 0usize;
            for (_, row) in expected
                .iter()
                .filter(|(key, _)| key.table() == table.table)
            {
                capture::matches(
                    self.tx,
                    binding,
                    table,
                    row.iter()
                        .map(|(column, value)| (column.as_str(), value.as_deref())),
                    None,
                )
                .await?;
                count += 1;
            }
            if table.table == "pin_management_records" {
                for original in &self.self_records {
                    let (executed, encoded) = written
                        .iter()
                        .find(|(e, _)| {
                            Some(e.action.receipt.binding.request.item_id.as_str())
                                == native::value(original, "item_id")
                        })
                        .ok_or(ManagementError::Changed)?;
                    let action = &executed.action;
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
                    count += 1;
                }
            }
            let actual = self
                .tx
                .query_one(sea_orm::Statement::from_string(
                    self.tx.get_database_backend(),
                    format!("SELECT COUNT(*) AS n FROM \"{}\"", table.table),
                ))
                .await
                .map_err(|_| ManagementError::Database)?
                .ok_or(ManagementError::Database)?
                .try_get::<i64>("", "n")
                .map_err(|_| ManagementError::Database)?;
            if usize::try_from(actual).ok() != Some(count) {
                return Err(ManagementError::Changed);
            }
        }
        if self.self_records.len() != written.len() {
            return Err(ManagementError::Changed);
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
