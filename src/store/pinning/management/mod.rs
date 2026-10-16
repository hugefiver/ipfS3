//! Stage 7 native evidence, durable records and transaction-owned management.
pub mod actions;
pub(crate) mod admission;
pub use admission::{ManagementJobReport, ManagementJobScope};
pub mod catalog;
pub mod native;
pub mod records;
pub mod schema;

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ManagementError {
    #[error("management database unavailable (details withheld)")]
    Database,
    #[error("management schema requires formal upgrade or repair")]
    Schema,
    #[error("management native evidence invalid")]
    InvalidEvidence,
    #[error("management complete inventory exceeds budget")]
    Budget,
    #[error("management confirmation does not match")]
    Confirmation,
    #[error("management native fence changed")]
    Changed,
    #[error("management evidence expired")]
    Expired,
    #[error("management operation requires local authorization")]
    Unauthorized,
}

pub type Result<T> = std::result::Result<T, ManagementError>;
