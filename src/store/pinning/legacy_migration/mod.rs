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
pub use apply::{apply, rollback};
pub use compat::{
    captured_provider, job_io_allowed, original_job_provider, release_history, resource_binding,
    resource_key,
};
pub use inventory::inventory;
pub use inventory::rollback_inventory;
pub use inventory::validate as validate_declaration;
pub use model::{
    Binding, Confirmation, Declaration, Maintenance, Namespace, NativeRow, Receipt, TableImage,
};
pub use receipt::{LegacyBinding, historical_keys, resolve_legacy_binding, source_is_migrated};

use crate::error::AppError;

fn refused(reason: &str) -> AppError {
    AppError::InvalidPinningRequest(format!("legacy migration refused: {reason}"))
}

pub const MAX_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_ROWS: usize = 10_000;
pub const MAX_NAMESPACES: usize = 32;
pub const SCHEMA: &str = "m20261007_000001_legacy_pinning_migration";
