//! Versioned data archive, not an authenticated plan or an execution capability.
//! Use these bounded entry points, not direct serde deserialization of model DTOs.

mod bounds;
mod review;
pub use review::encode_review;
pub(crate) use review::native_equal;

use serde::{Deserialize, Serialize};

use super::{DryRunPlan, MAX_RECORDS, model::*};

pub const WIRE_SCHEMA_VERSION: u32 = 2;
pub const MAX_WIRE_BYTES: usize = 16 * 1024 * 1024;
/// Every JSON value/container and object key is counted before DTO allocation.
pub const MAX_WIRE_NODES: usize = 32 * MAX_RECORDS;
pub const MAX_WIRE_DEPTH: usize = 64;

pub(crate) fn check_structure(input: &[u8]) -> Result<(), SafeError> {
    bounds::check(input)
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AppliedTrust {
    UntrustedClaimsOnly,
}

/// Narrow wire DTO: DryRunPlan is deliberately not directly deserializable.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanData {
    items: Vec<PlanEntry>,
    summary: Summary,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    schema_version: u32,
    applied_trust: AppliedTrust,
    request: PlanRequest,
    snapshot: SnapshotData,
    #[serde(deserialize_with = "Option::deserialize")]
    native: Option<NativeFrame>,
    plan: PlanData,
    #[serde(deserialize_with = "Option::deserialize")]
    confirmation: Option<Vec<String>>,
    /// Public derived points are proposal data only. Import confirmation checks
    /// them against complete locally stored receipts; they grant no DB authority.
    migration_points: Vec<crate::store::pinning::management::records::MigrationPoint>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotData {
    observed_at: i64,
    database_now: Option<chrono::DateTime<chrono::Utc>>,
    configuration: Option<ConfigurationBinding>,
    objects: Vec<ObjectSnapshot>,
    routes: Vec<RouteSnapshot>,
    targets: Vec<TargetSnapshot>,
    remotes: Vec<RemoteSnapshot>,
    historical_remotes: Vec<RemoteSnapshot>,
    unknown_resources: Vec<NativeKey>,
    ledger_domains: Vec<LedgerDomainSnapshot>,
    inventories: Vec<ProviderInventory>,
    #[serde(default)]
    root_references: Vec<RootReferenceSnapshot>,
    applied: Vec<AppliedItem>,
}

pub(crate) struct SnapshotRef<'a>(pub &'a Snapshot);
impl Serialize for SnapshotRef<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let snapshot = self.0;
        let mut state = serializer.serialize_struct("SnapshotRest", 13)?;
        state.serialize_field("observed_at", &snapshot.observed_at)?;
        state.serialize_field("database_now", &snapshot.database_now)?;
        state.serialize_field("configuration", &snapshot.configuration)?;
        state.serialize_field("objects", &snapshot.objects)?;
        state.serialize_field("routes", &snapshot.routes)?;
        state.serialize_field("targets", &snapshot.targets)?;
        state.serialize_field("remotes", &snapshot.remotes)?;
        state.serialize_field("historical_remotes", &snapshot.historical_remotes)?;
        state.serialize_field("unknown_resources", &snapshot.unknown_resources)?;
        state.serialize_field("ledger_domains", &snapshot.ledger_domains)?;
        state.serialize_field("inventories", &snapshot.inventories)?;
        state.serialize_field("root_references", &snapshot.root_references)?;
        state.serialize_field("applied", &snapshot.applied)?;
        state.end()
    }
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

/// Save exact original inputs and full plan/confirmation bindings. No IO.
/// Snapshot receipts, even if trusted by the caller, are untrusted when imported.
pub fn encode_plan(
    request: &PlanRequest,
    snapshot: &Snapshot,
    plan: &DryRunPlan,
    confirmation: Option<&Confirmation>,
) -> Result<Vec<u8>, SafeError> {
    encode_plan_with_session(
        request,
        snapshot,
        plan,
        confirmation.map(|confirmation| confirmation.candidate_item_ids.as_slice()),
        Default::default(),
    )
}
pub fn encode_plan_with_session(
    request: &PlanRequest,
    snapshot: &Snapshot,
    plan: &DryRunPlan,
    confirmation: Option<&[String]>,
    work: crate::store::pinning::management::records::SharedDecodeBudget,
) -> Result<Vec<u8>, SafeError> {
    let points = super::plan::planning_points_with_work(snapshot, work.clone())?;
    if super::plan::dry_run_view_with_work(request, snapshot.into(), &points, work.clone())?
        != *plan
    {
        return Err(SafeError::IdempotencyConflict);
    }
    if let Some(confirmation) = confirmation {
        super::plan::validate_acks(plan, confirmation)?;
    }
    #[derive(Serialize)]
    struct PlanRef<'a> {
        items: &'a [PlanEntry],
        summary: &'a Summary,
    }
    #[derive(Serialize)]
    struct DocumentRef<'a> {
        schema_version: u32,
        applied_trust: AppliedTrust,
        request: &'a PlanRequest,
        snapshot: SnapshotRef<'a>,
        native: Option<review::NativeProjection<'a>>,
        plan: PlanRef<'a>,
        confirmation: Option<&'a [String]>,
        migration_points: &'a [crate::store::pinning::management::records::MigrationPoint],
    }
    let document = DocumentRef {
        schema_version: WIRE_SCHEMA_VERSION,
        applied_trust: AppliedTrust::UntrustedClaimsOnly,
        request,
        snapshot: SnapshotRef(snapshot),
        native: snapshot
            .native_frame
            .as_ref()
            .map(review::NativeProjection::frame),
        plan: PlanRef {
            items: plan.items(),
            summary: plan.summary(),
        },
        confirmation,
        migration_points: &points,
    };
    work.lock()
        .map_err(|_| SafeError::InvalidInput)?
        .copy(&document)
        .map_err(|_| SafeError::LimitExceeded)?;
    let mut output = bounds::BoundedOutput::default();
    serde_json::to_writer(&mut output, &document).map_err(|_| SafeError::LimitExceeded)?;
    Ok(output.bytes)
}

/// First bound bytes/structure/depth without building a Value tree or DTO vectors.
/// Then reject unknown/duplicate fields, rebuild from ORIGINAL data and compare
/// the entire saved plan and confirmation. This proves consistency, not authority.
pub fn decode_plan(input: &[u8]) -> Result<ImportedPlan, SafeError> {
    decode_review(input, Default::default())
}

pub fn decode_review(
    input: &[u8],
    work: crate::store::pinning::management::records::SharedDecodeBudget,
) -> Result<ImportedPlan, SafeError> {
    if input.len() > MAX_WIRE_BYTES {
        return Err(SafeError::LimitExceeded);
    }
    let input = std::str::from_utf8(input).map_err(|_| SafeError::InvalidInput)?;
    let document: Document = work
        .lock()
        .map_err(|_| SafeError::InvalidInput)?
        .typed(input)
        .map_err(|error| {
            if error == crate::store::pinning::management::ManagementError::Budget {
                SafeError::LimitExceeded
            } else {
                SafeError::InvalidInput
            }
        })?;
    if document.schema_version != WIRE_SCHEMA_VERSION {
        return Err(SafeError::UnsupportedSchemaVersion);
    }
    let AppliedTrust::UntrustedClaimsOnly = document.applied_trust;
    if document.migration_points.len() > MAX_RECORDS {
        return Err(SafeError::LimitExceeded);
    }
    let rest = document.snapshot;
    let mut snapshot = Snapshot {
        observed_at: rest.observed_at,
        database_now: rest.database_now,
        native_frame: document.native,
        configuration: rest.configuration,
        objects: rest.objects,
        routes: rest.routes,
        targets: rest.targets,
        remotes: rest.remotes,
        historical_remotes: rest.historical_remotes,
        unknown_resources: rest.unknown_resources,
        ledger_domains: rest.ledger_domains,
        inventories: rest.inventories,
        root_references: rest.root_references,
        applied: rest.applied,
    };
    let plan = super::plan::restore_reviewed_plan(
        &document.request,
        (&snapshot).into(),
        &document.migration_points,
        document.plan.items,
        document.plan.summary,
        work.clone(),
    )?;
    let confirmation = document.confirmation;
    if let Some(acks) = &confirmation {
        super::plan::validate_acks(&plan, acks)?;
    }
    // Claims only participate in reproducing the archived dry-run, never in the
    // snapshot exposed for later revalidation. Only a trusted parent can attest
    // durable receipts atomically and provide an authoritative current snapshot.
    let untrusted_applied_claims = std::mem::take(&mut snapshot.applied);
    Ok(ImportedPlan {
        request: document.request,
        snapshot_without_applied: snapshot,
        untrusted_applied_claims,
        plan,
        confirmation,
        migration_points: document.migration_points,
        work,
    })
}

#[cfg(test)]
mod budget_tests {
    use super::*;
    #[test]
    fn partial_escaped_strings_prepay_scratch_with_finite_remaining_budget() {
        for prefix in ["{\"x\":\"", "{\""] {
            let input = format!("{prefix}{}", "\\u0061".repeat(256));
            let work = crate::store::pinning::management::records::SharedDecodeBudget::default();
            work.lock()
                .unwrap()
                .consume(0, MAX_WIRE_BYTES - input.len() - 256)
                .unwrap();
            assert!(matches!(
                decode_review(input.as_bytes(), work),
                Err(SafeError::LimitExceeded)
            ));
            assert!(matches!(
                decode_plan(input.as_bytes()),
                Err(SafeError::InvalidInput)
            ));
        }
    }
    #[test]
    fn exhausted_session_rejects_before_escaped_json_preflight() {
        let work = crate::store::pinning::management::records::SharedDecodeBudget::default();
        work.lock().unwrap().consume(0, MAX_WIRE_BYTES).unwrap();
        assert!(matches!(
            decode_review(b"{\"\\u0061\":\"\\u0062", work),
            Err(SafeError::LimitExceeded)
        ));
    }
}
