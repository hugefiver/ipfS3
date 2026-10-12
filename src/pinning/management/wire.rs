//! Versioned data archive, not an authenticated plan or an execution capability.
//! Use these bounded entry points, not direct serde deserialization of model DTOs.

mod bounds;

use serde::{Deserialize, Serialize};

use super::{DryRunPlan, MAX_RECORDS, dry_run, model::*, plan::validate_confirmation};

pub const WIRE_SCHEMA_VERSION: u32 = 1;
pub const MAX_WIRE_BYTES: usize = 16 * 1024 * 1024;
/// Every JSON value/container and object key is counted before DTO allocation.
pub const MAX_WIRE_NODES: usize = 32 * MAX_RECORDS;
pub const MAX_WIRE_DEPTH: usize = 64;

pub(crate) fn check_structure(input:&[u8])->Result<(),SafeError> { bounds::check(input) }

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AppliedTrust {
    UntrustedClaimsOnly,
}

/// Narrow wire DTO: DryRunPlan is deliberately not directly deserializable.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanData {
    plan_id: String,
    items: Vec<PlanEntry>,
    summary: Summary,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    schema_version: u32,
    applied_trust: AppliedTrust,
    request: PlanRequest,
    snapshot: Snapshot,
    plan: PlanData,
    #[serde(deserialize_with = "Option::deserialize")]
    confirmation: Option<Confirmation>,
    /// Public derived points are proposal data only. Import confirmation checks
    /// them against complete locally stored receipts; they grant no DB authority.
    #[serde(default, skip_serializing_if="Vec::is_empty")]
    migration_points:Vec<crate::store::pinning::management::records::MigrationPoint>,
}

/// Unauthenticated imported data. No mutable or receipt-bearing snapshot accessor.
/// Even a matching stored confirmation is not evidence of who authorized it.
pub struct ImportedPlan {
    request: PlanRequest,
    snapshot_without_applied: Snapshot,
    untrusted_applied_claims: Vec<AppliedItem>,
    plan: DryRunPlan,
    confirmation: Option<Confirmation>,
    migration_points:Vec<crate::store::pinning::management::records::MigrationPoint>,
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

    pub fn confirmation(&self) -> Option<&Confirmation> {
        self.confirmation.as_ref()
    }
    pub(crate) fn migration_points(&self)->&[crate::store::pinning::management::records::MigrationPoint] {&self.migration_points}
}

/// Save exact original inputs and full plan/confirmation bindings. No IO.
/// Snapshot receipts, even if trusted by the caller, are untrusted when imported.
pub fn encode_plan(
    request: &PlanRequest,
    snapshot: &Snapshot,
    plan: &DryRunPlan,
    confirmation: Option<&Confirmation>,
) -> Result<Vec<u8>, SafeError> {
    if dry_run(request, snapshot)? != *plan {
        return Err(SafeError::IdempotencyConflict);
    }
    if let Some(confirmation) = confirmation {
        validate_confirmation(plan, confirmation)?;
    }
    let document = Document {
        schema_version: WIRE_SCHEMA_VERSION,
        applied_trust: AppliedTrust::UntrustedClaimsOnly,
        request: request.clone(),
        snapshot: snapshot.clone(),
        plan: PlanData {
            plan_id: plan.plan_id().to_owned(),
            items: plan.items().to_vec(),
            summary: *plan.summary(),
        },
        confirmation: confirmation.cloned(),
        migration_points:super::plan::planning_points(snapshot)?,
    };
    let mut output = bounds::BoundedOutput::default();
    serde_json::to_writer(&mut output, &document).map_err(|_| SafeError::LimitExceeded)?;
    bounds::check(&output.bytes)?;
    Ok(output.bytes)
}

/// First bound bytes/structure/depth without building a Value tree or DTO vectors.
/// Then reject unknown/duplicate fields, rebuild from ORIGINAL data and compare
/// the entire saved plan and confirmation. This proves consistency, not authority.
pub fn decode_plan(input: &[u8]) -> Result<ImportedPlan, SafeError> {
    bounds::check(input)?;
    let mut document: Document =
        serde_json::from_slice(input).map_err(|_| SafeError::InvalidInput)?;
    if document.schema_version != WIRE_SCHEMA_VERSION {
        return Err(SafeError::UnsupportedSchemaVersion);
    }
    if document.migration_points.len()>MAX_RECORDS {return Err(SafeError::LimitExceeded);}
    let plan = super::plan::dry_run_with_points(&document.request, &document.snapshot,&document.migration_points)?;
    if document.plan.plan_id != plan.plan_id()
        || document.plan.items != plan.items()
        || document.plan.summary != *plan.summary()
    {
        return Err(SafeError::IdempotencyConflict);
    }
    if let Some(confirmation) = &document.confirmation {
        validate_confirmation(&plan, confirmation)?;
    }
    // Claims only participate in reproducing the archived dry-run, never in the
    // snapshot exposed for later revalidation. Only a trusted parent can attest
    // durable receipts atomically and provide an authoritative current snapshot.
    let untrusted_applied_claims = std::mem::take(&mut document.snapshot.applied);
    Ok(ImportedPlan {
        request: document.request,
        snapshot_without_applied: document.snapshot,
        untrusted_applied_claims,
        plan,
        confirmation: document.confirmation,
        migration_points:document.migration_points,
    })
}
