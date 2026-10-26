//! Immutable action bodies under the caller's authority fence. Native state is
//! resolved again for every predicate, including original Submit/history lineage.
use super::*;

pub(crate) struct CompletionReader<'a> {
    tx: &'a DatabaseTransaction,
    job: &'a pin_job::Model,
    actions: Vec<records::TrustedAppliedAction>,
    work: records::SharedDecodeBudget,
    runtime: Option<&'a crate::store::RuntimeTransaction>,
}

impl<'a> CompletionReader<'a> {
    pub(in crate::store::pinning) fn runtime_context(
        &self,
    ) -> Option<(&crate::store::RuntimeTransaction, records::DecodeWork<'_>)> {
        self.runtime
            .map(|tx| (tx, records::DecodeWork::shared(&self.work)))
    }
    pub(super) fn new(
        tx: &'a DatabaseTransaction,
        job: &'a pin_job::Model,
        actions: Vec<records::TrustedAppliedAction>,
        work: records::SharedDecodeBudget,
    ) -> Self {
        Self {
            tx,
            job,
            actions,
            work,
            runtime: None,
        }
    }

    pub(super) fn runtime(
        tx: &'a crate::store::RuntimeTransaction,
        job: &'a pin_job::Model,
        actions: Vec<records::TrustedAppliedAction>,
        work: records::SharedDecodeBudget,
    ) -> Self {
        Self {
            tx: tx.db(),
            job,
            actions,
            work,
            runtime: Some(tx),
        }
    }

    async fn validate(&self) -> AppResult<()> {
        if let Some(tx) = self.runtime {
            tx.check_schema(records::DecodeWork::shared(&self.work))
                .await
        } else {
            crate::store::pinning::management::schema::validate(self.tx)
                .await
                .map(|_| ())
                .map_err(safe)
        }
    }

    fn catalog(&self) -> ReceiptCatalog<'_> {
        match self.runtime {
            Some(tx) => ReceiptCatalog::Runtime(tx, records::DecodeWork::shared(&self.work)),
            None => ReceiptCatalog::full(self.tx),
        }
    }

    async fn permit(&self) -> AppResult<Option<JobPermit>> {
        if self.runtime.is_some() {
            return check_before_call_catalog(self.catalog(), self.job, self.work.clone())
                .await
                .map_err(safe);
        }
        let authority = lookup_from_actions(self.tx, self.job, &self.actions, self.work.clone())
            .await
            .map_err(safe)?;
        check_with_authority(self.tx, self.job, authority, self.work.clone())
            .await
            .map_err(safe)
    }

    pub(crate) fn is_bound(&self, tx: &DatabaseTransaction, job: &pin_job::Model) -> bool {
        std::ptr::eq(self.tx, tx) && std::ptr::eq(self.job, job)
    }

    pub(in crate::store::pinning) async fn submission_projection_allowed(
        &self,
        remote: &remote_pin::Model,
    ) -> AppResult<bool> {
        ledger::submission::projection_allowed_catalog(
            self.catalog(),
            self.job,
            remote,
            records::DecodeWork::shared(&self.work),
        )
        .await
    }

    /// Dispatch uses this reader's complete immutable action set under the same
    /// authority fence, while resolving the permit and source from fresh state.
    pub(crate) async fn call_and_source_allowed(
        &self,
        call: ManagedCall,
        config: Option<crate::pinning::management::local_config::ManagementConfigView<'_>>,
        idle_timeout_millis: Option<u64>,
    ) -> AppResult<bool> {
        self.validate().await?;
        let permit = self.permit().await?;
        if !call.allows(&self.job.operation, permit.as_ref()) {
            return Ok(false);
        }
        if matches!(call, ManagedCall::Submit)
            && let Some(JobPermit::Submit(authority)) = &permit
        {
            return fresh_source_catalog(
                self.catalog(),
                Some(authority),
                config,
                idle_timeout_millis,
                self.work.clone(),
            )
            .await;
        }
        Ok(true)
    }

    pub(crate) async fn projection_allowed(&self) -> AppResult<bool> {
        self.validate().await?;
        self.current_projection_allowed().await
    }

    async fn current_projection_allowed(&self) -> AppResult<bool> {
        let permit = self.permit().await?;
        Ok(matches!(
            permit,
            None | Some(JobPermit::Submit(_) | JobPermit::ExistingCleanup(_))
        ))
    }

    pub(crate) async fn fresh_source_allowed(
        &self,
        config: Option<crate::pinning::management::local_config::ManagementConfigView<'_>>,
        idle_timeout_millis: Option<u64>,
    ) -> AppResult<bool> {
        self.validate().await?;
        self.current_source_allowed(config, idle_timeout_millis)
            .await
    }

    /// One continuous read phase under the existing authority/lifecycle fences.
    /// Resolve current native authority once for this predicate; no permit is
    /// retained between calls or across a mutation boundary.
    pub(crate) async fn projection_and_source_allowed(
        &self,
        config: Option<crate::pinning::management::local_config::ManagementConfigView<'_>>,
        idle_timeout_millis: Option<u64>,
    ) -> AppResult<bool> {
        self.validate().await?;
        if self.runtime.is_some() {
            return projection_and_source_catalog(
                self.catalog(),
                self.job,
                config,
                idle_timeout_millis,
                self.work.clone(),
            )
            .await;
        }
        let authority = lookup_from_actions(self.tx, self.job, &self.actions, self.work.clone())
            .await
            .map_err(safe)?;
        projection_and_source_from_authority(
            self.tx,
            self.job,
            authority,
            config,
            idle_timeout_millis,
            self.work.clone(),
        )
        .await
    }

    async fn current_source_allowed(
        &self,
        config: Option<crate::pinning::management::local_config::ManagementConfigView<'_>>,
        idle_timeout_millis: Option<u64>,
    ) -> AppResult<bool> {
        if self.runtime.is_some() {
            let authority = lookup_catalog(self.catalog(), self.job, self.work.clone())
                .await
                .map_err(safe)?;
            return fresh_source_catalog(
                self.catalog(),
                authority.as_ref(),
                config,
                idle_timeout_millis,
                self.work.clone(),
            )
            .await;
        }
        let authority = lookup_from_actions(self.tx, self.job, &self.actions, self.work.clone())
            .await
            .map_err(safe)?;
        fresh_source_from_authority(
            self.tx,
            authority.as_ref(),
            config,
            idle_timeout_millis,
            self.work.clone(),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::TransactionTrait;

    #[tokio::test]
    async fn ordinary_remote_completion_keeps_readiness_live() {
        let fixture =
            super::super::super::tests::fixture_with_automatic("completion-readiness", true).await;
        let remote = remote_pin::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        let now = crate::store::database_clock::database_now(&fixture.db)
            .await
            .unwrap();
        let new_job = crate::store::pinning::jobs::reconcile_job(
            &remote.provider,
            &remote.cid,
            remote.epoch,
            now,
        );
        let crate::store::pinning::jobs::NewPinJob::Remote(job) = &new_job else {
            unreachable!()
        };
        let id = job.id.clone();
        crate::store::pinning::jobs::enqueue_job(&fixture.db, new_job)
            .await
            .unwrap();
        let job = pin_job::Entity::find_by_id(id)
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        assert!(lookup(&fixture.db, &job).await.unwrap().is_none());
        let tx = fixture.db.begin().await.unwrap();
        let _lifecycle =
            crate::store::pinning::leases::lock_worker_observation_lifecycle(&tx, &job)
                .await
                .unwrap();
        let reader =
            fence_worker_authority_with_work(&tx, &job, records::SharedDecodeBudget::default())
                .await
                .unwrap();
        assert!(reader.projection_allowed().await.unwrap());
        assert!(
            reader
                .call_and_source_allowed(ManagedCall::Observe, None, None)
                .await
                .unwrap()
        );
        assert!(reader.fresh_source_allowed(None, None).await.unwrap());
        assert!(
            reader
                .projection_and_source_allowed(None, None)
                .await
                .unwrap()
        );
        tx.execute_unprepared("UPDATE pin_management_readiness SET state='backfill' WHERE id=1")
            .await
            .unwrap();
        assert!(reader.projection_allowed().await.is_err());
        assert!(
            reader
                .call_and_source_allowed(ManagedCall::Observe, None, None)
                .await
                .is_err()
        );
        assert!(reader.fresh_source_allowed(None, None).await.is_err());
        assert!(
            reader
                .projection_and_source_allowed(None, None)
                .await
                .is_err()
        );
        tx.rollback().await.unwrap();
    }
}
