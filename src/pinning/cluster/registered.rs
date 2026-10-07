//! Registered provider boundary around the independently testable transport leaf.
#[path = "mod.rs"]
mod leaf;
pub(crate) use leaf::network;
pub use leaf::*;

#[path = "adapter.rs"]
mod adapter;
#[path = "config.rs"]
pub mod config;
pub use adapter::build_cluster_provider;
