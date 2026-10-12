//! Restore a locally confirmed historical binding; never issue a fresh grant.
use super::*;
use crate::pinning::management::{self as management, DryRunPlan, PlanStatus, Snapshot, wire::ImportedPlan};

/// Read-only restoration for exact replay, status and an applied-receipt scope
/// factory. Confirmation TTL and the current business frame are intentionally
/// not checked here. New apply still needs its own complete fresh/TTL fences;
/// a worker scope still needs every original trusted committed action receipt.
pub async fn load_confirmed_from_imported<C:ConnectionTrait>(
    db:&C,auth:&LocalAuthorization,imported:&ImportedPlan,
)->Result<ConfirmedBinding> {
    auth.check(db).await?;
    let rows=confirmation::rows(db,auth,&imported.request().plan_id).await?;
    load_from_rows(db,auth,imported,&rows).await
}

pub(super) async fn load_from_rows<C:ConnectionTrait>(db:&C,auth:&LocalAuthorization,imported:&ImportedPlan,rows:&[native::NativeRow])->Result<ConfirmedBinding> {
    let first=rows.first().ok_or(ManagementError::Confirmation)?;
    let before=native::value(first,"native_before").ok_or(ManagementError::InvalidEvidence)?;
    let (proposal,snapshot)=confirmation::original_proposal(before)?;
    let (_,mut expected)=validate_reviewed_original(&proposal,&snapshot,imported)?;
    if rows.iter().any(|row|native::value(row,"native_before")!=Some(before)) {return Err(ManagementError::Confirmation);}
    let encoded=native::value(first,"binding").ok_or(ManagementError::InvalidEvidence)?;
    // Stage stores (Proposal {binding,snapshot}, private) in both columns.
    // Explicit confirm CAS replaces only binding with (ConfirmedBinding, private)
    // under its write transaction/fresh-frame/TTL checks. A draft, including one
    // with an empty ACK set, cannot decode as that persisted confirmed shape.
    if encoded==before {return Err(ManagementError::Confirmation);}
    let binding=decode_confirmed_binding(encoded)?;
    let frame=binding.confirmation.native_frame.as_ref().ok_or(ManagementError::Confirmation)?;
    if binding.principal!=auth.principal||binding.database_scope!=auth.database_scope||binding.database_identity!=auth.database_identity
        ||frame.database_scope!=auth.database_scope||frame.database_identity!=auth.database_identity {return Err(ManagementError::Unauthorized);}
    expected.native_frame=proposal.confirmation.native_frame.clone();
    if binding.confirmation!=expected {return Err(ManagementError::Confirmation);}
    // Full original private tuples, same-plan membership, ACKs, timestamps and
    // every persisted binding must agree; current clock expiry is not a rewrite.
    confirmation::stored_binding(db,auth,&binding).await?;
    Ok(binding)
}

/// Shared original-document validation. The returned plan is still the frozen
/// proposal, not a revalidation against today's configuration or business rows.
pub(super) fn validate_reviewed_original(binding:&ConfirmedBinding,snapshot:&Snapshot,imported:&ImportedPlan)->Result<(DryRunPlan,Confirmation)> {
    if !imported.untrusted_applied_claims().is_empty() {return Err(ManagementError::Confirmation);}
    let reviewed=imported.confirmation().ok_or(ManagementError::Confirmation)?;
    if binding.request!=*imported.request()||!confirmation::public_equal(snapshot,imported.snapshot_without_applied())?
        ||management::plan::planning_points(snapshot).map_err(|_|ManagementError::InvalidEvidence)?.as_slice()!=imported.migration_points() {return Err(ManagementError::Confirmation);}
    let original=management::dry_run(&binding.request,snapshot).map_err(|_|ManagementError::InvalidEvidence)?;
    if original.plan_id()!=imported.plan().plan_id()||original.summary()!=imported.plan().summary()
        ||!confirmation::public_equal(original.items(),imported.plan().items())? {return Err(ManagementError::Confirmation);}
    let expected=original.confirmation(reviewed.candidate_item_ids.clone());
    if !confirmation::public_equal(&expected,reviewed)? {return Err(ManagementError::Confirmation);}
    let acknowledged=management::plan::validate_confirmation(&original,&expected).map_err(|_|ManagementError::Confirmation)?;
    if original.items().iter().any(|entry|matches!(entry.status,PlanStatus::Blocked(_))||entry.status==PlanStatus::Candidate&&!acknowledged.contains(entry.request.item_id.as_str())) {return Err(ManagementError::Confirmation);}
    Ok((original,expected))
}
