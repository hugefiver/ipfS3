//! Permanent allocation admission, separate from historical observation and settlement.
//! No provider calls, migration, quota grant, or management authority is issued here.
use sea_orm::{ConnectionTrait, DatabaseTransaction};

use super::records::{DecodeWork, SharedDecodeBudget};
use crate::{
    error::{AppError, AppResult},
    pinning::identity::ProviderRouteSnapshot,
};

pub(crate) fn work_error(error: super::ManagementError) -> AppError {
    match error {
        super::ManagementError::Budget => AppError::ManagementBudget,
        super::ManagementError::Database => AppError::Database(error.to_string()),
        _ => needs_attention(),
    }
}

pub(crate) async fn provider_registered_with_work<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    work: DecodeWork<'_>,
) -> AppResult<bool> {
    work.copy(provider).map_err(work_error)?;
    work.with(|work| work.nodes(1)).map_err(work_error)?;
    let row = db
        .query_one(sea_orm::Statement::from_sql_and_values(
            db.get_database_backend(),
            "SELECT EXISTS(SELECT 1 FROM pin_provider_routes WHERE provider=$1) AS present",
            [provider.into()],
        ))
        .await?
        .ok_or_else(needs_attention)?;
    Ok(row.try_get::<bool>("", "present")?)
}

pub(crate) mod authority;
mod domains;
mod intake;
mod locks;
mod managed_jobs;
mod observations;
mod retirement;
pub(crate) mod worker_observation;

pub(crate) use managed_jobs::ManagedCall;
pub(crate) use managed_jobs::managed_call_allowed_store;
pub use managed_jobs::{ManagementJobReport, ManagementJobScope};
pub(crate) use observations::{
    append_nonprojecting_delete_acknowledgement, append_nonprojecting_delete_error,
    append_nonprojecting_observation,
};

pub(crate) use intake::{
    lock_encoded_zip_intake_in_transaction, lock_intake_in_caller_transaction,
};

pub(crate) use domains::allocation_domain;
pub(crate) use domains::unregistered_legacy;
pub(crate) use locks::lock_in_caller_transaction;
#[cfg(test)]
pub(crate) use locks::lock_retirement_write_in_transaction;
pub(crate) use locks::lock_sqlite_writer_in_caller_transaction;

pub(crate) async fn lock_registration_in_transaction(
    tx: &DatabaseTransaction,
    provider: &str,
) -> AppResult<()> {
    locks::lock_existing_usage(tx, &[provider.to_owned()]).await?;
    locks::serialize(tx, locks::RetirementAccess::Admission).await?;
    locks::serialize_routes(tx, true).await?;
    locks::AdmissionSchema::read(tx).await.map(|_| ())
}

/// Read-only fail-fast hint. Only the final caller transaction proves admission.
pub(crate) async fn check_open<C: ConnectionTrait>(
    db: &C,
    routes: &[ProviderRouteSnapshot],
) -> AppResult<()> {
    let work = SharedDecodeBudget::default();
    check_open_with_work(db, routes, DecodeWork::shared(&work)).await
}
pub(crate) async fn check_open_with_work<C: ConnectionTrait>(
    db: &C,
    routes: &[ProviderRouteSnapshot],
    work: DecodeWork<'_>,
) -> AppResult<()> {
    if retirement::closed(db, routes, work).await? {
        return Err(retired());
    }
    Ok(())
}

/// R26 preservation is available only after a complete native projection.
/// The caller retains the final maintenance/authority fences for fresh apply.
pub(in crate::store::pinning) async fn check_r26_open<C: ConnectionTrait>(
    db: &C,
    admission: &crate::store::pinning::legacy_migration::R26Admission<'_>,
) -> AppResult<()> {
    let work = SharedDecodeBudget::default();
    if retirement::closed_r26(db, admission, DecodeWork::shared(&work)).await? {
        return Err(retired());
    }
    Ok(())
}

/// Call after bucket/owner/exact versions → leases → targets(created_at,id) →
/// remotes → usage, before taking any route row lock. Short shared retirement
/// and route table fences protect absence; ordinary births remain compatible.
/// Routes may come from verified R26 original points, never an opaque alias or
/// a new TOML interpretation. Complete historical prefixes retain only native
/// retired/domain protection, never a missing Stage7 retirement-write authority.
#[cfg(test)]
pub(crate) async fn lock_open_in_transaction(
    tx: &DatabaseTransaction,
    routes: &[ProviderRouteSnapshot],
) -> AppResult<()> {
    lock_in_caller_transaction(tx, routes).await
}
pub(crate) async fn lock_open_with_work<C: ConnectionTrait>(
    db: &C,
    routes: &[ProviderRouteSnapshot],
    work: DecodeWork<'_>,
) -> AppResult<()> {
    locks::lock_with_work(db, routes, work).await
}

/// For existing generic store primitives whose documented scope is caller-owned.
/// This function never creates or commits a transaction. An autocommit call is
/// NOT a barrier: gateway call sites must pass their existing business transaction.
pub(crate) async fn lock_provider_with_work<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    work: DecodeWork<'_>,
) -> AppResult<()> {
    let schema = locks::lock_admission_tables(db).await?;
    let routes = domains::registered_routes_in_schema_with_work(db, provider, work).await?;
    if routes.is_empty() {
        // Preserve the pre-registry low-level store API only on a genuinely empty
        // registry with no retirement. Missing current TOML identity is not proof.
        let unregistered = domains::unregistered_legacy_in_schema(db, provider, &schema).await?;
        if unregistered {
            return Ok(());
        }
        return Err(needs_attention());
    }
    locks::lock_route_rows_with_work(db, &routes, work).await?;
    let closed = retirement::closed_in_schema(db, &routes, &schema, false, work).await?;
    if closed {
        return Err(retired());
    }
    Ok(())
}

pub(in crate::store::pinning) async fn lock_runtime_provider(
    tx: &crate::store::RuntimeTransaction,
    provider: &str,
    work: DecodeWork<'_>,
) -> AppResult<()> {
    if tx.db().get_database_backend() != sea_orm::DatabaseBackend::Sqlite {
        return lock_provider_with_work(tx.db(), provider, work).await;
    }
    locks::serialize(tx.db(), locks::RetirementAccess::Admission).await?;
    locks::serialize_routes(tx.db(), false).await?;
    let catalog = tx.catalog(work);
    let schema = locks::AdmissionSchema::catalog(catalog).await?;
    let routes = domains::registered_routes_in_schema_with_work(tx.db(), provider, work).await?;
    if routes.is_empty() {
        return if domains::unregistered_legacy_in_schema(tx.db(), provider, &schema).await? {
            Ok(())
        } else {
            Err(needs_attention())
        };
    }
    locks::lock_route_rows_with_work(tx.db(), &routes, work).await?;
    if retirement::closed_with_role(&routes, &schema, false, None, work, catalog).await? {
        return Err(retired());
    }
    Ok(())
}

pub(in crate::store::pinning) async fn lock_runtime_routes(
    tx: &crate::store::RuntimeTransaction,
    routes: &[ProviderRouteSnapshot],
    work: DecodeWork<'_>,
) -> AppResult<()> {
    if routes.is_empty() {
        return Ok(());
    }
    locks::serialize(tx.db(), locks::RetirementAccess::Admission).await?;
    locks::serialize_routes(tx.db(), false).await?;
    let catalog = tx.catalog(work);
    let schema = locks::AdmissionSchema::catalog(catalog).await?;
    locks::lock_route_rows_with_work(tx.db(), routes, work).await?;
    if retirement::closed_with_role(routes, &schema, false, None, work, catalog).await? {
        return Err(retired());
    }
    Ok(())
}

#[cfg(test)]
pub(crate) async fn check_provider_open<C: ConnectionTrait>(
    db: &C,
    provider: &str,
) -> AppResult<()> {
    let work = SharedDecodeBudget::default();
    check_provider_with_work(db, provider, DecodeWork::shared(&work)).await
}
pub(crate) async fn check_provider_with_work<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    work: DecodeWork<'_>,
) -> AppResult<()> {
    check_provider_catalog(
        crate::store::pinning::legacy_migration::ReceiptCatalog::full(db),
        provider,
        work,
    )
    .await
}

pub(crate) async fn check_provider_catalog(
    catalog: crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
    provider: &str,
    work: DecodeWork<'_>,
) -> AppResult<()> {
    let db = &catalog;
    let schema = locks::AdmissionSchema::catalog(catalog).await?;
    let routes = domains::registered_routes_in_schema_with_work(db, provider, work).await?;
    if routes.is_empty() {
        let unregistered = domains::unregistered_legacy_in_schema(db, provider, &schema).await?;
        schema.revalidate_catalog(catalog).await?;
        if unregistered {
            return Ok(());
        }
        return Err(needs_attention());
    }
    let closed = retirement::closed_with_role(&routes, &schema, false, None, work, catalog).await?;
    schema.revalidate_catalog(catalog).await?;
    if closed {
        return Err(retired());
    }
    Ok(())
}

pub(in crate::store::pinning) async fn lock_quota_tables(
    db: &DatabaseTransaction,
) -> AppResult<()> {
    locks::serialize(db, locks::RetirementAccess::Admission).await?;
    locks::serialize_routes(db, false).await
}

pub(in crate::store::pinning) async fn check_quota_provider(
    window: &crate::store::pinning::quota::production::QuotaReadWindow<'_>,
    provider: &str,
    registered: bool,
    work: DecodeWork<'_>,
) -> AppResult<()> {
    let schema = locks::AdmissionSchema::quota(window).await?;
    let routes =
        domains::registered_routes_in_schema_with_work(window.db(), provider, work).await?;
    if routes.is_empty() {
        let unregistered =
            domains::unregistered_legacy_in_schema(window.db(), provider, &schema).await?;
        super::schema::validate_quota(window)
            .await
            .map_err(work_error)?;
        return if unregistered {
            Ok(())
        } else {
            Err(needs_attention())
        };
    }
    if registered {
        locks::lock_route_rows_with_work(window.db(), &routes, work).await?;
    }
    let closed = retirement::closed_quota(window, &routes, &schema, work).await?;
    super::schema::validate_quota(window)
        .await
        .map_err(work_error)?;
    if closed {
        return Err(retired());
    }
    Ok(())
}

/// Startup may preserve a closed route for historical GETs, but may not overwrite
/// its snapshot or introduce a new alias which could resurrect the allocation.
pub(crate) async fn registration_closed<C: ConnectionTrait>(
    db: &C,
    route: &ProviderRouteSnapshot,
) -> AppResult<bool> {
    let work = SharedDecodeBudget::default();
    registration_closed_with_work(db, route, DecodeWork::shared(&work)).await
}
pub(crate) async fn registration_closed_with_work<C: ConnectionTrait>(
    db: &C,
    route: &ProviderRouteSnapshot,
    work: DecodeWork<'_>,
) -> AppResult<bool> {
    registration_closed_catalog(
        crate::store::pinning::legacy_migration::ReceiptCatalog::full(db),
        route,
        work,
    )
    .await
}

pub(crate) async fn registration_closed_catalog(
    catalog: crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
    route: &ProviderRouteSnapshot,
    work: DecodeWork<'_>,
) -> AppResult<bool> {
    let schema = locks::AdmissionSchema::catalog(catalog).await?;
    let closed = retirement::closed_with_role(
        std::slice::from_ref(route),
        &schema,
        true,
        None,
        work,
        catalog,
    )
    .await?;
    schema.revalidate_catalog(catalog).await?;
    Ok(closed)
}

pub(in crate::store::pinning) async fn unregistered_catalog(
    catalog: crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
    provider: &str,
) -> AppResult<bool> {
    let schema = locks::AdmissionSchema::catalog(catalog).await?;
    let result = domains::unregistered_legacy_in_schema(&catalog, provider, &schema).await?;
    schema.revalidate_catalog(catalog).await?;
    Ok(result)
}

fn retired() -> AppError {
    AppError::InvalidPinningRequest("allocation domain is permanently retired".into())
}
pub(crate) fn needs_attention() -> AppError {
    AppError::InvalidPinningRequest(
        "retirement attribution needs attention; allocation denied".into(),
    )
}
