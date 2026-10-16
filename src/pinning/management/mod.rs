//! Stage 7 planning, exact native capture and explicit offline evidence preparation.

mod capture;
mod decision_scope;
mod model;
pub(crate) mod plan;
mod root_references;
mod semantics;
mod summary;
mod validate;
pub(crate) use validate::SnapshotView;
pub(crate) use validate::validate_snapshot_view_with_work;
pub mod db_snapshot;
pub mod evidence;
pub mod local_config;
pub mod wire;

pub use model::*;
pub use plan::*;
pub use validate::{
    MAX_ITEMS, MAX_PROVIDERS, MAX_RECORDS, MAX_TEXT_BYTES, validate_request, validate_snapshot,
};

#[cfg(test)]
mod tests;
