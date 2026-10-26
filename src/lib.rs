//! ipfs-s3-gateway library root.

#[cfg(test)]
extern crate self as ipfs_s3_gateway;

#[cfg(test)]
#[path = "../tests/support/stage7_management_actions.rs"]
pub mod stage7_management_test_support;

#[cfg(test)]
#[path = "../tests/support/sql_trace.rs"]
pub(crate) mod sql_trace;

pub mod auth;
pub mod config;
pub mod cors;
pub mod crypto;
pub mod error;
pub mod import;
pub mod kubo;
pub mod lifecycle;
pub mod pinning;
pub mod residency;
pub mod s3;
pub mod state;
pub mod store;
pub mod zip;
