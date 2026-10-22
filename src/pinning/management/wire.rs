//! Versioned data archive, not an authenticated plan or an execution capability.
//! Use these bounded entry points, not direct serde deserialization of model DTOs.

mod bounds;
pub mod conversion;
mod review;
pub mod v3;
pub use review::encode_review;
pub(crate) use review::native_equal;

use serde::Serialize;

use super::{DryRunPlan, MAX_RECORDS, model::*};

// Internal historical storage codec. Normal file IO uses v3::Reader/Writer.
pub const WIRE_SCHEMA_VERSION: u32 = 2;
pub const MAX_WIRE_BYTES: usize = 16 * 1024 * 1024;
/// Every JSON value/container and object key is counted before DTO allocation.
pub const MAX_WIRE_NODES: usize = 32 * MAX_RECORDS;
pub const MAX_WIRE_DEPTH: usize = 64;

pub(crate) fn check_structure(input: &[u8]) -> Result<(), SafeError> {
    bounds::check(input)
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum AppliedTrust {
    UntrustedClaimsOnly,
}

/// Unauthenticated imported data. No mutable or receipt-bearing snapshot accessor.
/// Even a matching stored confirmation is not evidence of who authorized it.
pub struct ImportedPlan {
    request: PlanRequest,
    snapshot_without_applied: Snapshot,
    untrusted_applied_claims: Vec<AppliedItem>,
    plan: DryRunPlan,
    confirmation: Option<Vec<String>>,
    work: crate::store::pinning::management::records::SharedDecodeBudget,
    migration_points: Vec<crate::store::pinning::management::records::MigrationPoint>,
}

impl ImportedPlan {
    pub fn request(&self) -> &PlanRequest {
        &self.request
    }

    /// All frozen snapshot fields except applied claims; never current-key data.
    /// Other imported fields also need parent authentication before execution.
    pub fn snapshot_without_applied(&self) -> &Snapshot {
        &self.snapshot_without_applied
    }

    /// Preserved for audit/reconstruction only. Do not install in trusted state.
    pub fn untrusted_applied_claims(&self) -> &[AppliedItem] {
        &self.untrusted_applied_claims
    }

    pub fn plan(&self) -> &DryRunPlan {
        &self.plan
    }

    pub fn confirmation(&self) -> Option<&[String]> {
        self.confirmation.as_deref()
    }
    pub fn session(&self) -> crate::store::pinning::management::records::SharedDecodeBudget {
        self.work.clone()
    }
    pub(crate) fn migration_points(
        &self,
    ) -> &[crate::store::pinning::management::records::MigrationPoint] {
        &self.migration_points
    }
}
