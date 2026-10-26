//! Explicit, offline capacity migration. No provider clients or network authority.
mod admission_schema;
mod apply;
mod closure_identity;
mod compat;
mod fence;
mod inventory;
mod management_closure;
mod management_points;
mod model;
mod mutation;
mod projection;
mod receipt;
mod receipt_schema;
mod resource_lifetime;
mod snapshot;

pub(crate) use admission_schema::admission_schema;
pub(in crate::store::pinning) use admission_schema::receipt_schema;
pub use apply::{apply, rollback};
pub(crate) use compat::job_io_allowed_catalog;
pub(in crate::store::pinning) use compat::job_io_allowed_from_catalog;
#[cfg(test)]
pub(crate) use compat::job_io_allowed_from_loaded;
#[cfg(test)]
pub(crate) use compat::job_io_allowed_with_work;
pub(crate) use compat::job_receipt_selectors;
pub(in crate::store::pinning) use compat::original_job_provider_catalog;
pub(in crate::store::pinning) use compat::release_history_catalog;
#[cfg(test)]
pub(crate) use compat::resource_binding_with_work;
pub(in crate::store::pinning) use compat::resource_key_from_loaded;
pub(crate) use compat::resource_receipt_candidates_with_work;
pub(in crate::store::pinning) use compat::resource_receipt_selectors;
pub use compat::{
    captured_provider, job_io_allowed, original_job_provider, release_history, resource_binding,
    resource_key,
};
pub(in crate::store::pinning) use compat::{
    identity_binding_from_catalog, resource_binding_from_catalog,
};
pub(crate) use compat::{original_job_provider_with_work, release_history_with_work};
pub(crate) use compat::{
    release_history_current_catalog, resource_binding_current_catalog, resource_key_catalog,
};
pub(crate) use compat::{resource_binding_catalog, resource_key_budgeted};
pub use inventory::inventory;
pub use inventory::rollback_inventory;
pub use inventory::validate as validate_declaration;
pub use model::{
    Binding, Confirmation, Declaration, Maintenance, Namespace, NativeRow, Receipt, TableImage,
};
pub(in crate::store::pinning) use projection::R26Admission;
pub(crate) use receipt::ReceiptCatalog;
pub(crate) use receipt::admission_historical_keys;
pub(in crate::store::pinning) use receipt::admission_historical_keys_with_catalog;
pub(crate) use receipt::historical_job_keys;
pub(in crate::store::pinning) use receipt::historical_job_keys_from_catalog;
#[cfg(test)]
pub(crate) use receipt::historical_job_keys_from_loaded;
pub(crate) use receipt::resolve_legacy_binding_with_work;
pub(crate) use receipt::source_is_migrated_catalog;
pub(crate) use receipt::source_is_migrated_with_work;
pub(in crate::store::pinning) use receipt::work::Work as ReceiptWork;
pub use receipt::{LegacyBinding, historical_keys, resolve_legacy_binding, source_is_migrated};
pub(crate) use receipt::{historical_keys_catalog, resolve_with_catalog};
pub(crate) async fn historical_keys_budgeted<C: sea_orm::ConnectionTrait>(
    db: &C,
    table: &str,
    provider: &str,
    cid: Option<&str>,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> crate::store::pinning::management::Result<Vec<String>> {
    receipt::historical_keys_with_work(db, table, provider, cid, &work).await
}

pub(in crate::store::pinning) async fn historical_keys_with_catalog(
    table: &str,
    provider: &str,
    cid: Option<&str>,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
    catalog: ReceiptCatalog<'_>,
) -> crate::store::pinning::management::Result<Vec<String>> {
    if matches!(catalog, ReceiptCatalog::Full(_)) {
        return historical_keys_budgeted(&catalog, table, provider, cid, work).await;
    }
    let selector =
        crate::store::pinning::management::records::locator::AuthoritySelector::LegacyResource(
            table.into(),
            provider.into(),
            cid.map(str::to_owned),
        );
    let candidates = catalog.load(&[selector], work).await?;
    receipt::historical_keys_from_catalog(table, provider, cid, &candidates, &work, work, catalog)
        .await
}

use crate::error::AppError;

fn refused(reason: &str) -> AppError {
    AppError::InvalidPinningRequest(format!("legacy migration refused: {reason}"))
}

pub const MAX_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_ROWS: usize = 10_000;
pub const MAX_NAMESPACES: usize = 32;
pub const SCHEMA: &str = "m20261007_000001_legacy_pinning_migration";
