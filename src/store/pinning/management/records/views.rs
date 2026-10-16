//! Read-only binding adapters. Frame ownership is independent of confirmation overlays.
use super::*;
use crate::pinning::management::{ConfigurationBinding, PlanEntry};

pub type SharedDecodeBudget = std::sync::Arc<std::sync::Mutex<ReadSession>>;

/// Call-local work only. This context carries no binding, membership or grant.
#[derive(Clone, Copy)]
pub(crate) struct DecodeWork<'a>(&'a SharedDecodeBudget);
impl<'a> From<BindingView<'a>> for DecodeWork<'a> {
    fn from(view: BindingView<'a>) -> Self {
        view.decode_work()
    }
}
impl<'a> DecodeWork<'a> {
    pub(crate) fn budget(self) -> &'a SharedDecodeBudget {
        self.0
    }
    pub(crate) fn shared(work: &'a SharedDecodeBudget) -> Self {
        Self(work)
    }
    pub(crate) fn with<T>(self, f: impl FnOnce(&mut ReadSession) -> Result<T>) -> Result<T> {
        f(&mut *self
            .0
            .lock()
            .map_err(|_| ManagementError::InvalidEvidence)?)
    }
    #[track_caller]
    pub(crate) fn copy<T: Serialize + ?Sized>(self, value: &T) -> Result<()> {
        self.0
            .lock()
            .map_err(|_| ManagementError::InvalidEvidence)?
            .copy(value)
    }
    pub(crate) fn copy_out(self, row: RowView<'_>) -> Result<native::NativeRow> {
        self.copy(&row)?;
        Ok(row
            .iter()
            .map(|(column, value)| (column.clone(), value.clone()))
            .collect())
    }
    pub(in crate::store::pinning::management::records) fn index_copy<T: Serialize + ?Sized>(
        self,
        value: &T,
    ) -> Result<()> {
        self.with(|work| work.index_copy(value))
    }
    pub(crate) fn index_cells(self, cells: usize, bytes: usize) -> Result<()> {
        self.with(|work| work.index_cells(cells, bytes))
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ConfirmationView<'a> {
    pub plan_id: &'a String,
    pub bindings: &'a Vec<PlanEntry>,
    pub candidate_item_ids: &'a [String],
    pub configuration: &'a Option<ConfigurationBinding>,
}

#[derive(Clone, Copy)]
pub(crate) struct BindingView<'a> {
    pub(crate) rows: Option<&'a archive::ArchivedRows>,
    pub(super) work: &'a SharedDecodeBudget,
    pub request: &'a PlanRequest,
    pub confirmation: ConfirmationView<'a>,
    pub principal: &'a String,
    pub database_scope: &'a String,
    pub database_identity: &'a String,
    pub confirmed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl<'a> From<&'a ConfirmedBinding> for BindingView<'a> {
    fn from(binding: &'a ConfirmedBinding) -> Self {
        Self {
            rows: Some(binding.archived_rows()),
            work: &binding.work,
            request: &binding.request,
            confirmation: ConfirmationView {
                plan_id: &binding.confirmation.plan_id,
                bindings: &binding.confirmation.bindings,
                candidate_item_ids: &binding.confirmation.candidate_item_ids,
                configuration: &binding.confirmation.configuration,
            },
            principal: &binding.principal,
            database_scope: &binding.database_scope,
            database_identity: &binding.database_identity,
            confirmed_at: binding.confirmed_at,
            expires_at: binding.expires_at,
        }
    }
}
impl<'a> From<&BindingView<'a>> for BindingView<'a> {
    fn from(binding: &BindingView<'a>) -> Self {
        *binding
    }
}
impl<'a> BindingView<'a> {
    pub(crate) fn frozen_rows(self) -> Result<row::FrozenRows<'a>> {
        if let Some(rows) = self.rows {
            return Ok(row::FrozenRows::Archive(rows));
        }
        Err(ManagementError::Confirmation)
    }
    pub(crate) fn decode_work(&self) -> DecodeWork<'a> {
        DecodeWork(self.work)
    }
    pub(crate) fn with_decode_work<T>(
        &self,
        f: impl FnOnce(&mut ReadSession) -> Result<T>,
    ) -> Result<T> {
        self.decode_work().with(f)
    }
    pub(crate) fn with_work(mut self, work: &'a SharedDecodeBudget) -> Self {
        self.work = work;
        self
    }
    pub(crate) fn decode_shared<T: serde::de::DeserializeOwned + Send + Sync + 'static>(
        &self,
        input: &str,
    ) -> Result<std::sync::Arc<T>> {
        self.work
            .lock()
            .map_err(|_| ManagementError::InvalidEvidence)?
            .typed_shared(input)
    }
    pub(crate) fn decode_route(
        &self,
        input: &str,
    ) -> Result<std::sync::Arc<crate::pinning::identity::ProviderRouteSnapshot>> {
        self.decode_shared(input).map_err(|error| {
            if error == ManagementError::Budget {
                error
            } else {
                ManagementError::Confirmation
            }
        })
    }
    pub(crate) fn decode<T: serde::de::DeserializeOwned>(&self, input: &str) -> Result<T> {
        self.work
            .lock()
            .map_err(|_| ManagementError::InvalidEvidence)?
            .typed(input)
    }
    #[track_caller]
    pub(crate) fn copy<T: Serialize + ?Sized>(&self, value: &T) -> Result<()> {
        self.decode_work().copy(value)
    }
}
impl PartialEq for BindingView<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.request == other.request
            && self.principal == other.principal
            && self.database_scope == other.database_scope
            && self.database_identity == other.database_identity
            && self.confirmed_at == other.confirmed_at
            && self.expires_at == other.expires_at
            && self.confirmation.plan_id == other.confirmation.plan_id
            && self.confirmation.bindings == other.confirmation.bindings
            && self.confirmation.candidate_item_ids == other.confirmation.candidate_item_ids
            && match (self.frozen_rows(), other.frozen_rows()) {
                (Ok(left), Ok(right)) => {
                    left.scope() == right.scope()
                        && left.identity() == right.identity()
                        && left
                            .all_rows()
                            .map(|(table, row)| (&table.table, row))
                            .eq(right.all_rows().map(|(table, row)| (&table.table, row)))
                }
                (Err(_), Err(_)) => true,
                _ => false,
            }
            && self.confirmation.configuration == other.confirmation.configuration
    }
}
impl PartialEq<&ConfirmedBinding> for BindingView<'_> {
    fn eq(&self, other: &&ConfirmedBinding) -> bool {
        *self == BindingView::from(*other)
    }
}
