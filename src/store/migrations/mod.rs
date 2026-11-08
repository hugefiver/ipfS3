pub mod baseline;
pub(crate) mod bridge;
pub mod history;
pub(crate) mod legacy_receipts;
pub mod m20261027_000001_schema_baseline;
pub mod m20261108_000001_current_pinning_config;
pub(crate) mod registry;

#[cfg(any(test, feature = "historical-fixtures"))]
#[path = "../../../tests/support/historical_migrations/mod.rs"]
mod fixtures;
#[cfg(any(test, feature = "historical-fixtures"))]
pub use fixtures::*;
#[cfg(test)]
mod current_pinning_tests;
#[cfg(test)]
mod stage3_tests;
