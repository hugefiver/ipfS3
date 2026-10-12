//! Permanent allocation admission, separate from historical observation and settlement.
//! No provider calls, migration, quota grant, or management authority is issued here.
use sea_orm::{ConnectionTrait, DatabaseTransaction};

use crate::{error::{AppError, AppResult}, pinning::identity::ProviderRouteSnapshot};

mod domains;
mod locks;
mod retirement;
mod intake;
mod managed_jobs;
mod observations;

pub use managed_jobs::{ManagementJobScope, ManagementJobReport};
pub(crate) use managed_jobs::{ManagedCall, managed_call_allowed};
pub(crate) use observations::{append_nonprojecting_observation, append_nonprojecting_delete_acknowledgement, append_nonprojecting_delete_error};

pub(crate) use intake::{lock_intake_in_caller_transaction, lock_encoded_zip_intake_in_transaction};

pub(crate) use domains::allocation_domain;
pub(crate) use domains::unregistered_legacy;
pub(crate) use locks::lock_in_caller_transaction;
#[cfg(test)]
pub(crate) use locks::lock_retirement_write_in_transaction;
pub(crate) use locks::lock_sqlite_writer_in_caller_transaction;

pub(crate) async fn lock_registration_in_transaction(tx: &DatabaseTransaction, provider: &str) -> AppResult<()> {
    locks::lock_existing_usage(tx, &[provider.to_owned()]).await?;
    let schema = locks::serialize(tx, locks::RetirementAccess::Admission).await?;
    locks::serialize_routes(tx, true).await?;
    schema.revalidate(tx).await
}

/// Read-only fail-fast hint. Only the final caller transaction proves admission.
pub(crate) async fn check_open<C: ConnectionTrait>(db: &C, routes: &[ProviderRouteSnapshot]) -> AppResult<()> {
    if retirement::closed(db, routes).await? { return Err(retired()); }
    Ok(())
}

/// Call after bucket/owner/exact versions → leases → targets(created_at,id) →
/// remotes → usage, before taking any route row lock. Short shared retirement
/// and route table fences protect absence; ordinary births remain compatible.
/// Routes may come from verified R26 original points, never an opaque alias or
/// a new TOML interpretation. Complete historical prefixes retain only native
/// retired/domain protection, never a missing Stage7 retirement-write authority.
pub(crate) async fn lock_open_in_transaction(tx: &DatabaseTransaction, routes: &[ProviderRouteSnapshot]) -> AppResult<()> {
    lock_in_caller_transaction(tx, routes).await
}

/// For existing generic store primitives whose documented scope is caller-owned.
/// This function never creates or commits a transaction. An autocommit call is
/// NOT a barrier: gateway call sites must pass their existing business transaction.
pub(crate) async fn lock_provider_in_caller_transaction<C: ConnectionTrait>(db: &C, provider: &str) -> AppResult<()> {
    let schema = locks::lock_admission_tables(db).await?;
    let routes = domains::registered_routes_in_schema(db, provider).await?;
    if routes.is_empty() {
        // Preserve the pre-registry low-level store API only on a genuinely empty
        // registry with no retirement. Missing current TOML identity is not proof.
        let unregistered = domains::unregistered_legacy_in_schema(db, provider, &schema).await?;
        schema.revalidate(db).await?;
        if unregistered { return Ok(()); }
        return Err(needs_attention());
    }
    locks::lock_route_rows(db, &routes).await?;
    let closed = retirement::closed_in_schema(db, &routes, &schema, false).await?;
    schema.revalidate(db).await?;
    if closed { return Err(retired()); }
    Ok(())
}

pub(crate) async fn check_provider_open<C: ConnectionTrait>(db: &C, provider: &str) -> AppResult<()> {
    let routes = domains::registered_routes(db, provider).await?;
    if routes.is_empty() {
        if domains::unregistered_legacy(db, provider).await? { return Ok(()); }
        return Err(needs_attention());
    }
    check_open(db, &routes).await
}

/// Startup may preserve a closed route for historical GETs, but may not overwrite
/// its snapshot or introduce a new alias which could resurrect the allocation.
pub(crate) async fn registration_closed<C: ConnectionTrait>(db: &C, route: &ProviderRouteSnapshot) -> AppResult<bool> {
    let schema = locks::AdmissionSchema::read(db).await?;
    let closed = retirement::closed_in_schema(db, std::slice::from_ref(route), &schema, true).await?;
    schema.revalidate(db).await?;
    Ok(closed)
}

fn retired() -> AppError { AppError::InvalidPinningRequest("allocation domain is permanently retired".into()) }
pub(crate) fn needs_attention() -> AppError { AppError::InvalidPinningRequest("retirement attribution needs attention; allocation denied".into()) }
