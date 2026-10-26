//! SQLite worker hints from a closed, operation-owned read transaction.
use super::{authority, work_error};
use crate::{
    error::AppResult,
    pinning::identity::{ProviderIdentity, ProviderRouteSnapshot},
    store::{
        entities::pin_job,
        pinning::{
            leases, ledger, legacy_migration,
            management::{
                actions::job_authority::{self, JobPermit},
                records, schema,
            },
        },
    },
};
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, DatabaseTransaction, TransactionTrait,
};

/// Only this module constructs and ends the transaction. No caller can retain
/// a catalog qualification across a write, provider call, or another phase.
pub(crate) struct WorkerReadWindow {
    tx: WindowOwner,
    versions: Vec<String>,
}

enum WindowOwner {
    Full(DatabaseTransaction),
    Runtime(
        crate::store::RuntimeTransaction,
        records::SharedDecodeBudget,
    ),
}

impl WorkerReadWindow {
    pub(in crate::store::pinning) fn versions(&self) -> &[String] {
        &self.versions
    }
    pub(in crate::store::pinning) async fn unregistered(&self, provider: &str) -> AppResult<bool> {
        let schema = super::locks::AdmissionSchema::observed(self).await?;
        let result =
            super::domains::unregistered_legacy_in_schema(self.db(), provider, &schema).await?;
        schema::validate_observed(self).await.map_err(work_error)?;
        Ok(result)
    }
    pub(in crate::store::pinning) fn db(&self) -> &DatabaseTransaction {
        match &self.tx {
            WindowOwner::Full(tx) => tx,
            WindowOwner::Runtime(tx, _) => tx.db(),
        }
    }

    fn catalog<'a>(
        &'a self,
        work: records::DecodeWork<'a>,
    ) -> legacy_migration::ReceiptCatalog<'a> {
        match &self.tx {
            WindowOwner::Runtime(tx, _) => tx.catalog(work),
            WindowOwner::Full(_) => legacy_migration::ReceiptCatalog::Observed(self),
        }
    }

    pub(in crate::store::pinning) async fn runtime_versions(
        &self,
    ) -> Option<AppResult<Vec<String>>> {
        match &self.tx {
            WindowOwner::Runtime(tx, work) => {
                Some(tx.schema_versions(records::DecodeWork::shared(work)).await)
            }
            WindowOwner::Full(_) => None,
        }
    }

    async fn begin_store(
        store: &crate::store::Store,
        work: records::SharedDecodeBudget,
    ) -> AppResult<Self> {
        let tx = store
            .begin_runtime(records::DecodeWork::shared(&work))
            .await?;
        let versions = match tx.schema_versions(records::DecodeWork::shared(&work)).await {
            Ok(versions) => versions,
            Err(error) => {
                tx.rollback().await?;
                return Err(error);
            }
        };
        Ok(Self {
            tx: WindowOwner::Runtime(tx, work),
            versions,
        })
    }

    async fn close(self) -> AppResult<()> {
        match self.tx {
            WindowOwner::Full(tx) => {
                tx.rollback().await?;
                Ok(())
            }
            WindowOwner::Runtime(tx, _) => tx.rollback().await,
        }
    }

    async fn begin(db: &DatabaseConnection) -> AppResult<Self> {
        let tx = db.begin().await?;
        let versions = match legacy_migration::receipt_schema(&tx).await {
            Ok(versions) => versions,
            Err(error) => {
                tx.rollback().await?;
                return Err(error);
            }
        };
        if versions.last().map(String::as_str) != Some(schema::VERSION) {
            tx.rollback().await?;
            return Err(super::needs_attention());
        }
        Ok(Self {
            tx: WindowOwner::Full(tx),
            versions,
        })
    }
}

/// Observations are branch hints, never dispatch or mutation permits.
pub(crate) struct ClaimedObservation {
    pub management_observation: bool,
    pub route: Option<ProviderRouteSnapshot>,
    pub ordinary: ledger::JobAdmission,
}

pub(crate) async fn observe_claimed_job(
    db: &DatabaseConnection,
    job: &pin_job::Model,
    identity: Option<&ProviderIdentity>,
    work: records::SharedDecodeBudget,
) -> AppResult<ClaimedObservation> {
    if db.get_database_backend() != DatabaseBackend::Sqlite {
        return claimed_generic(db, job, identity, work).await;
    }
    let window = WorkerReadWindow::begin(db).await?;
    let result = Box::pin(claimed(&window, job, identity, work)).await;
    window.close().await?;
    result
}

async fn claimed(
    window: &WorkerReadWindow,
    job: &pin_job::Model,
    identity: Option<&ProviderIdentity>,
    work: records::SharedDecodeBudget,
) -> AppResult<ClaimedObservation> {
    claimed_catalog(
        window.catalog(records::DecodeWork::shared(&work)),
        job,
        identity,
        work.clone(),
    )
    .await
}

pub(crate) async fn claimed_runtime(
    tx: &crate::store::RuntimeTransaction,
    job: &pin_job::Model,
    identity: Option<&ProviderIdentity>,
    work: records::SharedDecodeBudget,
) -> AppResult<ClaimedObservation> {
    if tx.db().get_database_backend() != DatabaseBackend::Sqlite {
        return claimed_generic(tx.db(), job, identity, work).await;
    }
    claimed_catalog(
        tx.catalog(records::DecodeWork::shared(&work)),
        job,
        identity,
        work.clone(),
    )
    .await
}

async fn claimed_catalog(
    catalog: legacy_migration::ReceiptCatalog<'_>,
    job: &pin_job::Model,
    identity: Option<&ProviderIdentity>,
    work: records::SharedDecodeBudget,
) -> AppResult<ClaimedObservation> {
    use records::locator::AuthoritySelector;
    let mut selectors = vec![AuthoritySelector::Job(job.id.clone())];
    if let Some(lease) = &job.lease_id {
        selectors.push(AuthoritySelector::Lease(lease.clone()));
    }
    if let Some(target) = &job.target_id {
        selectors.push(AuthoritySelector::Target(target.clone()));
    }
    let context = records::DecodeWork::shared(&work);
    let candidates = catalog
        .load(&selectors, context)
        .await
        .map_err(work_error)?;
    let actions = authority::actions_from_catalog_candidates(catalog, &candidates, work.clone())
        .await
        .map_err(work_error)?;
    let (managed, route) =
        job_authority::admission_from_catalog(catalog, job, &actions, work.clone()).await?;
    if managed.is_some() || route.is_some() {
        return Ok(managed_hint(managed, route));
    }
    let candidates = catalog
        .load(&legacy_migration::job_receipt_selectors(job), context)
        .await
        .map_err(work_error)?;
    let ordinary = if job.operation == "submit" {
        if !legacy_migration::job_io_allowed_from_catalog(job, &candidates, context, catalog)
            .await
            .map_err(work_error)?
        {
            ledger::JobAdmission::IoDenied
        } else if ledger::route_matches_catalog(
            catalog,
            &job.provider,
            &job.cid,
            identity,
            &candidates,
            context,
        )
        .await?
        {
            ledger::JobAdmission::Allowed
        } else {
            ledger::JobAdmission::RouteDenied
        }
    } else {
        ledger::job_io_and_route_from_candidates(job, identity, &candidates, context, catalog)
            .await?
    };
    Ok(ClaimedObservation {
        management_observation: false,
        route: None,
        ordinary,
    })
}

pub(crate) async fn claimed_generic<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    identity: Option<&ProviderIdentity>,
    work: records::SharedDecodeBudget,
) -> AppResult<ClaimedObservation> {
    let (managed, route) = job_authority::admission_with_route(db, job, work.clone()).await?;
    if managed.is_some() || route.is_some() {
        return Ok(managed_hint(managed, route));
    }
    let context = records::DecodeWork::shared(&work);
    let ordinary = if job.operation == "submit" {
        ordinary_submit(db, job, identity, context).await?
    } else {
        ledger::job_io_and_route_with_work(db, job, identity, context).await?
    };
    Ok(ClaimedObservation {
        management_observation: false,
        route: None,
        ordinary,
    })
}

fn managed_hint(
    managed: Option<JobPermit>,
    route: Option<ProviderRouteSnapshot>,
) -> ClaimedObservation {
    let (management_observation, route) = match managed {
        Some(JobPermit::ReadOnly(authority) | JobPermit::LedgerOnly(authority)) => {
            (true, Some(authority.route))
        }
        Some(JobPermit::Submit(authority) | JobPermit::ExistingCleanup(authority)) => {
            (false, Some(authority.route))
        }
        None => (false, route),
    };
    ClaimedObservation {
        management_observation,
        route,
        ordinary: ledger::JobAdmission::Allowed,
    }
}

async fn ordinary_submit<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    identity: Option<&ProviderIdentity>,
    work: records::DecodeWork<'_>,
) -> AppResult<ledger::JobAdmission> {
    if !legacy_migration::job_io_allowed_with_work(db, job, work)
        .await
        .map_err(work_error)?
    {
        return Ok(ledger::JobAdmission::IoDenied);
    }
    Ok(
        if ledger::route_matches(db, &job.provider, &job.cid, identity).await? {
            ledger::JobAdmission::Allowed
        } else {
            ledger::JobAdmission::RouteDenied
        },
    )
}

pub(crate) struct ReconcileObservation {
    pub management_observation: bool,
    pub snapshot: Option<leases::RemoteWorkSnapshot>,
}

pub(crate) async fn observe_reconcile(
    db: &DatabaseConnection,
    job: &pin_job::Model,
    work: records::SharedDecodeBudget,
) -> AppResult<ReconcileObservation> {
    if db.get_database_backend() != DatabaseBackend::Sqlite {
        let managed = job_authority::check_before_call_with_work(db, job, work)
            .await
            .map_err(work_error)?;
        let management_observation = matches!(
            managed,
            Some(JobPermit::ReadOnly(_) | JobPermit::LedgerOnly(_))
        );
        let snapshot = if management_observation {
            None
        } else {
            leases::remote_work_snapshot(db, &job.provider, &job.cid).await?
        };
        return Ok(ReconcileObservation {
            management_observation,
            snapshot,
        });
    }
    let window = WorkerReadWindow::begin(db).await?;
    let result = Box::pin(reconcile(&window, job, work)).await;
    window.close().await?;
    result
}

async fn reconcile(
    window: &WorkerReadWindow,
    job: &pin_job::Model,
    work: records::SharedDecodeBudget,
) -> AppResult<ReconcileObservation> {
    let catalog = window.catalog(records::DecodeWork::shared(&work));
    let candidates = catalog
        .load(
            &[records::locator::AuthoritySelector::Job(job.id.clone())],
            records::DecodeWork::shared(&work),
        )
        .await
        .map_err(work_error)?;
    let actions = authority::actions_from_catalog_candidates(catalog, &candidates, work.clone())
        .await
        .map_err(work_error)?;
    let (managed, _) =
        job_authority::admission_from_catalog(catalog, job, &actions, work.clone()).await?;
    let management_observation = matches!(
        managed,
        Some(JobPermit::ReadOnly(_) | JobPermit::LedgerOnly(_))
    );
    let snapshot = if management_observation {
        None
    } else {
        snapshot(
            window,
            &job.provider,
            &job.cid,
            records::DecodeWork::shared(&work),
        )
        .await?
    };
    Ok(ReconcileObservation {
        management_observation,
        snapshot,
    })
}

async fn snapshot(
    window: &WorkerReadWindow,
    provider: &str,
    cid: &str,
    work: records::DecodeWork<'_>,
) -> AppResult<Option<leases::RemoteWorkSnapshot>> {
    let candidates = authority::load_observed(
        window,
        &legacy_migration::resource_receipt_selectors(provider, cid),
        work,
    )
    .await
    .map_err(work_error)?;
    let key = legacy_migration::resource_key_from_loaded(
        provider,
        cid,
        &candidates,
        work,
        window.catalog(work),
    )
    .await
    .map_err(work_error)?;
    leases::remote_work_snapshot_at_key(window.db(), &key, cid).await
}

pub(crate) async fn observe_snapshot(
    db: &DatabaseConnection,
    provider: &str,
    cid: &str,
    work: records::SharedDecodeBudget,
) -> AppResult<Option<leases::RemoteWorkSnapshot>> {
    if db.get_database_backend() != DatabaseBackend::Sqlite {
        return leases::remote_work_snapshot(db, provider, cid).await;
    }
    let window = WorkerReadWindow::begin(db).await?;
    let result = snapshot(&window, provider, cid, records::DecodeWork::shared(&work)).await;
    window.close().await?;
    result
}

pub(crate) async fn observe_failed_cleanup(
    db: &DatabaseConnection,
    provider: &str,
    cid: &str,
    work: records::SharedDecodeBudget,
) -> AppResult<(bool, Option<leases::RemoteWorkSnapshot>)> {
    if db.get_database_backend() != DatabaseBackend::Sqlite {
        let cleanup = ledger::cleanup_allowed_with_work(
            db,
            provider,
            cid,
            records::DecodeWork::shared(&work),
        )
        .await?;
        let snapshot = if cleanup {
            leases::remote_work_snapshot(db, provider, cid).await?
        } else {
            None
        };
        return Ok((cleanup, snapshot));
    }
    let window = WorkerReadWindow::begin(db).await?;
    let result = Box::pin(failed_cleanup(
        &window,
        provider,
        cid,
        records::DecodeWork::shared(&work),
    ))
    .await;
    window.close().await?;
    result
}

async fn failed_cleanup(
    window: &WorkerReadWindow,
    provider: &str,
    cid: &str,
    work: records::DecodeWork<'_>,
) -> AppResult<(bool, Option<leases::RemoteWorkSnapshot>)> {
    let candidates = authority::load_observed(
        window,
        &legacy_migration::resource_receipt_selectors(provider, cid),
        work,
    )
    .await
    .map_err(work_error)?;
    let cleanup = ledger::cleanup_observed(window, provider, cid, work, candidates).await?;
    let snapshot = if cleanup {
        snapshot(window, provider, cid, work).await?
    } else {
        None
    };
    Ok((cleanup, snapshot))
}

#[cfg(test)]
mod tests;

pub(crate) async fn observe_claimed_job_store(
    store: &crate::store::Store,
    job: &pin_job::Model,
    identity: Option<&ProviderIdentity>,
    work: records::SharedDecodeBudget,
) -> AppResult<ClaimedObservation> {
    if store.db().get_database_backend() != DatabaseBackend::Sqlite {
        return observe_claimed_job(store.db(), job, identity, work).await;
    }
    let window = WorkerReadWindow::begin_store(store, work.clone()).await?;
    let result = Box::pin(claimed(&window, job, identity, work)).await;
    window.close().await?;
    result
}

pub(crate) async fn observe_reconcile_store(
    store: &crate::store::Store,
    job: &pin_job::Model,
    work: records::SharedDecodeBudget,
) -> AppResult<ReconcileObservation> {
    if store.db().get_database_backend() != DatabaseBackend::Sqlite {
        return observe_reconcile(store.db(), job, work).await;
    }
    let window = WorkerReadWindow::begin_store(store, work.clone()).await?;
    let result = Box::pin(reconcile(&window, job, work)).await;
    window.close().await?;
    result
}

pub(crate) async fn observe_snapshot_store(
    store: &crate::store::Store,
    provider: &str,
    cid: &str,
    work: records::SharedDecodeBudget,
) -> AppResult<Option<leases::RemoteWorkSnapshot>> {
    if store.db().get_database_backend() != DatabaseBackend::Sqlite {
        return observe_snapshot(store.db(), provider, cid, work).await;
    }
    let window = WorkerReadWindow::begin_store(store, work.clone()).await?;
    let result = snapshot(&window, provider, cid, records::DecodeWork::shared(&work)).await;
    window.close().await?;
    result
}

pub(crate) async fn observe_failed_cleanup_store(
    store: &crate::store::Store,
    provider: &str,
    cid: &str,
    work: records::SharedDecodeBudget,
) -> AppResult<(bool, Option<leases::RemoteWorkSnapshot>)> {
    if store.db().get_database_backend() != DatabaseBackend::Sqlite {
        return observe_failed_cleanup(store.db(), provider, cid, work).await;
    }
    let window = WorkerReadWindow::begin_store(store, work.clone()).await?;
    let result = Box::pin(failed_cleanup(
        &window,
        provider,
        cid,
        records::DecodeWork::shared(&work),
    ))
    .await;
    window.close().await?;
    result
}
