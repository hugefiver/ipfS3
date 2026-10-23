//! Original SQL suppliers. Frozen manifests alone determine historical membership.
pub(crate) use super::session::ReadSession as Budget;
use super::*;
#[cfg(test)]
use std::sync::Mutex;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, MutexGuard},
};

pub(super) fn key(row: &native::NativeRow) -> Result<native::NativeKey> {
    Ok(native::NativeKey {
        table: super::super::schema::RECORDS.into(),
        values: ["database_scope", "plan_id", "item_id", "kind"]
            .into_iter()
            .map(|column| {
                native::value(row, column)
                    .map(str::to_owned)
                    .ok_or(ManagementError::InvalidEvidence)
            })
            .collect::<Result<_>>()?,
    })
}
pub(super) fn anchor_key(binding: &ConfirmedBinding) -> Result<native::NativeKey> {
    let item = binding
        .request
        .items
        .iter()
        .map(|item| &item.item_id)
        .min()
        .ok_or(ManagementError::Confirmation)?;
    Ok(native::NativeKey {
        table: super::super::schema::RECORDS.into(),
        values: vec![
            binding.database_scope.clone(),
            binding.request.plan_id.clone(),
            item.clone(),
            "action".into(),
        ],
    })
}
pub(super) fn view_anchor_key(binding: BindingView<'_>) -> Result<native::NativeKey> {
    let item = binding
        .request
        .items
        .iter()
        .map(|item| &item.item_id)
        .min()
        .ok_or(ManagementError::Confirmation)?;
    binding.copy(&(
        super::super::schema::RECORDS,
        [
            binding.database_scope,
            binding.request.plan_id.as_str(),
            item.as_str(),
            "action",
        ],
    ))?;
    Ok(native::NativeKey {
        table: super::super::schema::RECORDS.into(),
        values: vec![
            binding.database_scope.into(),
            binding.request.plan_id.clone(),
            item.clone(),
            "action".into(),
        ],
    })
}
pub(super) fn is_draft(raw: &str, work: &mut Budget) -> Result<bool> {
    if archive::is_shared_binding(raw) {
        return Ok(false);
    }
    let binding: archive::StoredBinding = work.typed(
        raw.strip_prefix(archive::BINDING)
            .ok_or(ManagementError::InvalidEvidence)?,
    )?;
    Ok(matches!(binding, archive::StoredBinding::Draft(..)))
}

pub(crate) struct Evidence {
    scope: String,
    identity: String,
    rows: Arc<BTreeMap<native::NativeKey, Arc<native::NativeRow>>>,
    bytes: usize,
    reserved_rows: usize,
    locators: BTreeSet<native::NativeKey>,
    pub(super) work: SharedDecodeBudget,
    scoped_reads: Vec<native::scope::ScopedRead>,
}
/// Raw member data only. Every reuse checks the complete member set and cells.
pub(super) struct FrozenSourceIndex {
    scope: String,
    identity: String,
    rows: Arc<BTreeMap<native::NativeKey, Arc<native::NativeRow>>>,
    bytes: usize,
}
impl Evidence {
    fn cached_frozen(rows: FrozenRows<'_>, work: &SharedDecodeBudget) -> Result<Option<Self>> {
        let mut budget = work.lock().map_err(|_| ManagementError::InvalidEvidence)?;
        budget.check()?;
        let count = rows
            .all_rows()
            .filter(|(table, row)| {
                table.table == super::super::schema::RECORDS && row.cell("kind") == Some("action")
            })
            .count();
        let cached = budget
            .frozen_sources
            .iter()
            .find(|index| {
                index.scope == rows.scope()
                    && index.identity == rows.identity()
                    && index.rows.len() == count
                    && index.rows.iter().all(|(key, original)| {
                        rows.row_key(&key.table, &key.values)
                            .is_some_and(|current| current == *original.as_ref())
                    })
            })
            .map(|index| (index.rows.clone(), index.bytes));
        let Some((index, bytes)) = cached else {
            return Ok(None);
        };
        budget.copy(&(rows.scope(), rows.identity()))?;
        Ok(Some(Self {
            scoped_reads: Vec::new(),
            scope: rows.scope().into(),
            identity: rows.identity().into(),
            rows: index,
            bytes,
            reserved_rows: 0,
            locators: BTreeSet::new(),
            work: work.clone(),
        }))
    }
    fn remember_frozen(&self) -> Result<()> {
        let mut budget = self.budget()?;
        budget.copy(&(&self.scope, &self.identity))?;
        budget.nodes(2)?;
        budget.frozen_sources.push(FrozenSourceIndex {
            scope: self.scope.clone(),
            identity: self.identity.clone(),
            rows: self.rows.clone(),
            bytes: self.bytes,
        });
        Ok(())
    }
    pub(crate) fn from_frozen_with_work(
        rows: FrozenRows<'_>,
        work: SharedDecodeBudget,
    ) -> Result<Self> {
        if let FrozenRows::Frame(frame) = rows {
            return Self::from_frame_with_work(frame, work);
        }
        if let Some(source) = Self::cached_frozen(rows, &work)? {
            return Ok(source);
        }
        DecodeWork::shared(&work).copy(&(rows.scope(), rows.identity()))?;
        let mut result = Self {
            scoped_reads: Vec::new(),
            scope: rows.scope().into(),
            identity: rows.identity().into(),
            rows: Arc::default(),
            bytes: 0,
            reserved_rows: 0,
            locators: BTreeSet::new(),
            work,
        };
        for (table, row) in rows.all_rows().filter(|(table, row)| {
            table.table == super::super::schema::RECORDS && row.cell("kind") == Some("action")
        }) {
            let mut budget = result.budget()?;
            budget.copy(&(
                table.table.as_str(),
                [
                    row.cell("database_scope"),
                    row.cell("plan_id"),
                    row.cell("item_id"),
                    row.cell("kind"),
                ],
            ))?;
            let key = native::NativeKey {
                table: table.table.clone(),
                values: table
                    .primary_key
                    .iter()
                    .map(|column| {
                        row.cell(column)
                            .map(str::to_owned)
                            .ok_or(ManagementError::InvalidEvidence)
                    })
                    .collect::<Result<_>>()?,
            };
            let row = archive::retain_view(row, &mut budget)?;
            drop(budget);
            result.insert_shared(key, row)?;
        }
        result.remember_frozen()?;
        Ok(result)
    }
    pub(super) fn budget(&self) -> Result<MutexGuard<'_, Budget>> {
        self.work
            .lock()
            .map_err(|_| ManagementError::InvalidEvidence)
    }
    pub(super) fn check_remaining(&self, rows: usize, bytes: usize) -> Result<()> {
        if self
            .rows
            .len()
            .checked_add(self.locators.len())
            .and_then(|n| n.checked_add(self.reserved_rows))
            .and_then(|n| n.checked_add(rows))
            .is_none_or(|n| n > native::MAX_ROWS)
            || self
                .bytes
                .checked_add(bytes)
                .is_none_or(|n| n > native::MAX_BYTES)
        {
            return Err(ManagementError::Budget);
        }
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn from_frame(frame: &NativeFrame) -> Result<Self> {
        Self::from_frame_with_work(frame, Arc::default())
    }
    pub(super) fn from_frame_with_work(
        frame: &NativeFrame,
        work: SharedDecodeBudget,
    ) -> Result<Self> {
        frame.validate()?;
        if let Some(source) = Self::cached_frozen(frame.into(), &work)? {
            return Ok(source);
        }
        work.lock()
            .map_err(|_| ManagementError::InvalidEvidence)?
            .copy(&(&frame.database_scope, &frame.database_identity))?;
        let mut result = Self {
            scoped_reads: Vec::new(),
            scope: frame.database_scope.clone(),
            identity: frame.database_identity.clone(),
            rows: Arc::default(),
            bytes: 0,
            reserved_rows: 0,
            locators: BTreeSet::new(),
            work,
        };
        for (key, hidden) in frame.private_rows() {
            if key.table != super::super::schema::RECORDS
                || key.values.len() != 4
                || key.values[3] != "action"
            {
                continue;
            }
            if native::value(hidden, "binding").is_none()
                || native::value(hidden, "native_before").is_none()
            {
                return Err(ManagementError::InvalidEvidence);
            }
            let row = {
                let mut budget = result.budget()?;
                budget.nodes(1)?;
                budget.copy(key)?;
                archive::frame_row(frame, key, &mut budget)?
            };
            result.insert_shared(key.clone(), row)?;
        }
        result.remember_frozen()?;
        Ok(result)
    }
    fn insert(&mut self, key: native::NativeKey, row: native::NativeRow) -> Result<()> {
        let row = archive::retain_row(row, &mut *self.budget()?)?;
        if key.table == super::super::schema::RECORDS {
            archive::remember_sql_row(row.clone(), &mut *self.budget()?)?;
        }
        self.insert_shared(key, row)
    }
    fn insert_shared(&mut self, key: native::NativeKey, row: Arc<native::NativeRow>) -> Result<()> {
        if self.rows.contains_key(&key) {
            return Err(ManagementError::InvalidEvidence);
        }
        let mut bytes = row.values().flatten().try_fold(0usize, |sum, value| {
            sum.checked_add(value.len()).ok_or(ManagementError::Budget)
        })?;
        let located = self.locators.contains(&key);
        if located {
            bytes = bytes
                .checked_sub(key.values.iter().map(String::len).sum())
                .ok_or(ManagementError::InvalidEvidence)?;
        }
        self.check_remaining(usize::from(!located), bytes)?;
        self.bytes += bytes;
        self.locators.remove(&key);
        if Arc::strong_count(&self.rows) > 1 {
            let mut budget = self.budget()?;
            budget.nodes(1 + self.rows.len())?;
            for key in self.rows.keys() {
                budget.copy(key)?;
            }
        }
        Arc::make_mut(&mut self.rows).insert(key, row);
        Ok(())
    }
    pub(super) fn row(&self, key: &native::NativeKey) -> Result<&native::NativeRow> {
        self.rows
            .get(key)
            .map(AsRef::as_ref)
            .ok_or(ManagementError::InvalidEvidence)
    }
    pub(super) fn action_rows(&self) -> impl Iterator<Item = &native::NativeRow> {
        self.rows.values().map(AsRef::as_ref)
    }
    pub(super) fn archive_row(&self, key: &archive::Key) -> Result<Arc<native::NativeRow>> {
        self.rows
            .iter()
            .find(|(candidate, _)| candidate.table == key.0 && candidate.values == key.1)
            .map(|(_, row)| row.clone())
            .ok_or(ManagementError::InvalidEvidence)
    }
    pub(super) fn archive_row_ref(&self, key: &archive::Key) -> Result<&native::NativeRow> {
        self.rows
            .iter()
            .find(|(candidate, _)| candidate.table == key.0 && candidate.values == key.1)
            .map(|(_, row)| row.as_ref())
            .ok_or(ManagementError::InvalidEvidence)
    }
    pub(super) fn archive_members<'a>(
        &'a self,
        scope: &'a str,
        plan: &'a str,
    ) -> impl Iterator<Item = &'a native::NativeRow> {
        self.rows
            .iter()
            .filter(move |(key, _)| {
                key.table == super::super::schema::RECORDS
                    && key.values[0] == scope
                    && key.values[1] == plan
            })
            .map(|(_, row)| row.as_ref())
    }
    pub(super) fn archive_entries<'a>(
        &'a self,
        scope: &'a str,
        plan: &'a str,
    ) -> impl Iterator<Item = (&'a native::NativeKey, &'a Arc<native::NativeRow>)> {
        self.rows.iter().filter(move |(key, _)| {
            key.table == super::super::schema::RECORDS
                && key.values[0] == scope
                && key.values[1] == plan
        })
    }
    pub(super) fn archive_scope(&self, scope: &str, identity: &str) -> Result<()> {
        if self.scope != scope || self.identity != identity {
            return Err(ManagementError::Unauthorized);
        }
        Ok(())
    }
    pub(super) fn retirement_action(&self, row: &native::NativeRow) -> Result<&native::NativeRow> {
        let values = ["database_scope", "plan_id", "item_id"]
            .into_iter()
            .map(|column| {
                native::value(row, column)
                    .map(str::to_owned)
                    .ok_or(ManagementError::InvalidEvidence)
            })
            .chain(std::iter::once(Ok("action".into())))
            .collect::<Result<_>>()?;
        self.row(&native::NativeKey {
            table: super::super::schema::RECORDS.into(),
            values,
        })
    }
    pub(super) fn validate_draft(&self, row: &native::NativeRow) -> Result<()> {
        archive::validate_draft(self, row, &mut *self.budget()?)
    }
    pub(super) fn binding(
        &self,
        row: &native::NativeRow,
        work: &mut Budget,
    ) -> Result<ConfirmedBinding> {
        archive::read_binding(self, row, work)
    }
    pub(super) fn read_binding(&self, row: &native::NativeRow) -> Result<ReadBinding> {
        archive::load_confirmed(self, row, &mut *self.budget()?)
    }
}
pub(super) type ReadBinding = archive::ConfirmedPlan;
#[cfg(test)]
mod directory_budget_tests;
#[cfg(test)]
mod inspection_tests;
mod load;
#[cfg(test)]
mod read_tests;
#[cfg(test)]
mod tests;
