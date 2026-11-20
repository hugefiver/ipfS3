//! ipfs-s3-gateway library root.

#![recursion_limit = "256"]

#[cfg(test)]
extern crate self as ipfs_s3_gateway;

#[cfg(test)]
#[path = "../tests/support/sql_trace.rs"]
pub(crate) mod sql_trace;

pub mod access;
pub mod auth;
pub mod config;
pub mod cors;
pub mod crypto;
pub mod error;
pub mod iam;
pub mod import;
pub mod kubo;
pub mod lifecycle;
pub mod metrics;
pub mod pinning;
pub mod residency;
pub mod s3;
pub mod state;
pub mod store;
pub mod zip;
