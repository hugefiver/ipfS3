//! Allocation obligations are independent of whether a provider job was born.
use super::*;
use crate::pinning::identity::ProviderRouteSnapshot;
use crate::pinning::management::{Intent, ObjectCapture, ProviderMode, Requirements};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Contract {
    provider_mode: String,
    content_mode: String,
    content_cids: BTreeSet<String>,
    candidate_order: Vec<String>,
    candidates: BTreeMap<String, Option<ProviderRouteSnapshot>>,
    management_requirements: Requirements,
}

pub(super) fn validate(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    op: &OperationRead,
    intent: &Intent,
    capture: &ObjectCapture,
    lease: &records::RowHandle,
    requirement: &records::RowHandle,
) -> Result<()> {
    let frame = binding.frozen_rows()?;
    let summary = capture
        .object
        .read_evidence
        .as_ref()
        .ok_or(ManagementError::Confirmation)?;
    let count = accounting::count(summary)?;
    let stored = frame
        .row("objects", &[&capture.object.identity.object_id])
        .ok_or(ManagementError::Confirmation)?;
    let cid = cell(stored, "cid")?;
    let id = cell(lease, "id")?;
    let mode = match intent.requirements.mode {
        ProviderMode::One => "one",
        ProviderMode::All => "all",
    };
    let contract: Contract = binding.decode(cell(requirement, "contract")?)?;
    if native::value(lease, "provider_mode") != Some(mode)
        || native::value(lease, "content_mode") != Some("full")
        || contract.provider_mode != mode
        || contract.content_mode != "full"
        || contract.management_requirements != intent.requirements
        || contract.content_cids.len() != 1
        || !contract.content_cids.contains(cid)
        || contract.candidate_order.len() != intent.requirements.provider_ids.len()
        || contract.candidates.len() != contract.candidate_order.len()
        || native::value(requirement, "owner_object_id") != Some(&capture.owner_identity.object_id)
        || action
            .decision
            .as_ref()
            .is_none_or(|decision| decision.stored_bytes != count.bytes)
        || op.before(&capture::key("pin_leases", id))?.is_some()
        || op
            .before(&capture::key("pin_lease_requirements", id))?
            .is_some()
    {
        return Err(ManagementError::Confirmation);
    }
    let mut candidates = BTreeSet::new();
    for (provider, domain) in intent
        .requirements
        .provider_ids
        .iter()
        .zip(&contract.candidate_order)
    {
        let route = capture
            .requirement_routes
            .iter()
            .find(|route| &route.identity.provider_id == provider)
            .and_then(|route| route.original.as_ref())
            .ok_or(ManagementError::Confirmation)?;
        let original = frame
            .row("pin_provider_routes", &[domain])
            .ok_or(ManagementError::Confirmation)?;
        if contract.candidates.get(domain).and_then(Option::as_ref) != Some(route)
            || binding.decode_route(cell(original, "snapshot")?)?.as_ref() != route
            || !candidates.insert(domain.as_str())
        {
            return Err(ManagementError::Confirmation);
        }
    }
    if action.targets.len()
        != match intent.requirements.mode {
            ProviderMode::One => 1,
            ProviderMode::All => candidates.len(),
        }
    {
        return Err(ManagementError::Confirmation);
    }
    let mut targets = BTreeSet::new();
    for target_id in &action.targets {
        let key = capture::key("pin_lease_targets", target_id);
        let target = op.after(&key)?.ok_or(ManagementError::Confirmation)?;
        let provider = cell(target, "provider")?;
        let target_cid = cell(target, "cid")?;
        if op.before(&key)?.is_some()
            || native::value(target, "lease_id") != Some(id)
            || !candidates.contains(provider)
            || !targets.insert(provider)
            || cell(target, "logical_size")?.parse::<u64>().ok() != Some(count.bytes)
            || crate::pinning::provider::canonical_resource_cid(target_cid)
                .ok()
                .as_deref()
                != Some(&capture.object.identity.cid)
            || cell(target, "id")?
                != crate::store::pinning::leases::target_id(id, provider, target_cid)
            || !matches!(
                cell(target, "state")?,
                "waiting" | "submitted" | "pinned" | "degraded" | "quota_waiting" | "quota_blocked"
            )
        {
            return Err(ManagementError::Confirmation);
        }
        let remote_key = facts::Key(
            "remote_pins".into(),
            vec![provider.into(), target_cid.into()],
        );
        let ledger_key = facts::Key("remote_pin_ledger".into(), remote_key.1.clone());
        let remote = op.after(&remote_key)?;
        let ledger = op.after(&ledger_key)?;
        if desired(target) {
            let remote = remote.ok_or(ManagementError::Confirmation)?;
            let ledger = ledger.ok_or(ManagementError::Confirmation)?;
            if cell(remote, "cid_size")?.parse::<u64>().ok() != Some(count.bytes)
                || cell(remote, "status")? == "absent"
                || cell(ledger, "effect")? == "absent"
            {
                return Err(ManagementError::Confirmation);
            }
            let route = binding.decode_route(cell(ledger, "route")?)?;
            if let Some(old) = op.before(&ledger_key)? {
                // Reuse retains the original resource lifetime, not today's registration.
                if cell(
                    op.before(&remote_key)?
                        .ok_or(ManagementError::Confirmation)?,
                    "status",
                )? != "absent"
                    && (cell(old, "route")? != cell(ledger, "route")?
                        || native::value(old, "created_at") != native::value(ledger, "created_at"))
                {
                    return Err(ManagementError::Confirmation);
                }
            }
            if !capture
                .requirement_routes
                .iter()
                .chain(&capture.historical_routes)
                .chain(capture.remote_route.iter())
                .any(|candidate| candidate.original.as_ref() == Some(route.as_ref()))
            {
                return Err(ManagementError::Confirmation);
            }
        }
        if let Some(remote) = remote
            && cell(remote, "cid_size")?.parse::<u64>().ok() != Some(count.bytes)
        {
            return Err(ManagementError::Confirmation);
        }
    }
    if intent.requirements.mode == ProviderMode::All && targets != candidates {
        return Err(ManagementError::Confirmation);
    }
    Ok(())
}
