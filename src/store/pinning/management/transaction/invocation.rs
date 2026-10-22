use super::*;

/// Exclusive, lazy SQLite connection retention for one confirmation invocation.
pub(crate) struct ConfirmationEngine<'db> {
    db: &'db DatabaseConnection,
    engine: Option<engine::Engine>,
    enabled: bool,
}

impl<'db> ConfirmationEngine<'db> {
    pub(crate) fn new(db: &'db DatabaseConnection) -> Self {
        Self {
            db,
            engine: None,
            enabled: db.get_database_backend() == DatabaseBackend::Sqlite,
        }
    }

    pub(crate) async fn begin(
        &mut self,
        limit: Duration,
        cancel: CancellationToken,
    ) -> std::result::Result<ConfirmationTransaction<'_, 'db>, TransactionFailure> {
        let deadline = Deadline::new(limit, cancel);
        let eligible = std::mem::replace(&mut self.enabled, false);
        let retained = self.engine.take();
        let transaction = ManagementTransaction::begin_engine(
            self.db,
            deadline,
            retained,
            (IsolationLevel::ReadCommitted, AccessMode::ReadWrite),
        )
        .await?;
        Ok(ConfirmationTransaction {
            transaction,
            slot: self,
            eligible,
        })
    }
}

/// The borrow prevents another page from acquiring this invocation's engine
/// before the caller has observed completion and reclaimed exclusive ownership.
pub(crate) struct ConfirmationTransaction<'slot, 'db> {
    transaction: ManagementTransaction,
    slot: &'slot mut ConfirmationEngine<'db>,
    eligible: bool,
}

impl std::ops::Deref for ConfirmationTransaction<'_, '_> {
    type Target = ManagementTransaction;
    fn deref(&self) -> &Self::Target {
        &self.transaction
    }
}
impl std::ops::DerefMut for ConfirmationTransaction<'_, '_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.transaction
    }
}

impl ConfirmationTransaction<'_, '_> {
    pub(crate) async fn commit(self) -> std::result::Result<(), TransactionFailure> {
        self.complete(lifecycle::Request::Commit).await
    }

    pub(crate) async fn rollback(self) -> std::result::Result<(), TransactionFailure> {
        self.complete(lifecycle::Request::Rollback).await
    }

    async fn complete(
        self,
        request: lifecycle::Request,
    ) -> std::result::Result<(), TransactionFailure> {
        let shared = &self.transaction.shared;
        shared.request(request).await;
        let outcome = shared.finished().await;
        let result = match (request, outcome) {
            (lifecycle::Request::Commit, lifecycle::Outcome::Committed) => Ok(()),
            (lifecycle::Request::Rollback, outcome) if outcome.failure().rollback_confirmed => {
                Ok(())
            }
            (_, outcome) => Err(outcome.failure()),
        };
        let state = shared.state.lock().await;
        let mut engine = shared.engine.lock().await;
        if self.eligible
            && state.reclaimable
            && !state.busy
            && shared.deadline.reason().is_none()
            && !shared.abandoned.is_cancelled()
        {
            self.slot.engine = engine.take();
            self.slot.enabled = self.slot.engine.is_some();
        }
        result
    }
}

#[cfg(test)]
#[path = "reuse_tests.rs"]
mod tests;
