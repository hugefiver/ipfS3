use super::*;

pub(super) fn target(
    frame: &NativeFrame,
    target: &TargetEvidence,
    lease: &LeaseEvidence,
    contract: Option<&NativeRow>,
    work: &SharedDecodeBudget,
) -> Result<(
    Option<RouteIdentity>,
    Option<Requirements>,
    Vec<FrozenCandidate>,
)> {
    let Some(row) = contract else {
        // A ledger's original route can support explicit observation, but does
        // not invent a pre-birth lease's frozen requirements.
        let key = [("id".into(), target.id.clone())].into();
        let point = super::super::legacy::resolve(
            frame,
            "pin_lease_targets",
            &key,
            &target.allocation,
            work,
        )?;
        let decoded = if point.is_none() {
            frame
                .original_value(
                    "remote_pin_ledger",
                    &[&target.allocation, &target.original_cid],
                    "route",
                )
                .map(|raw| decode::<ProviderRouteSnapshot>(raw, work))
                .transpose()?
        } else {
            None
        };
        let original = point
            .as_ref()
            .and_then(|point| point.historical_route.as_ref())
            .or(decoded.as_deref());
        return Ok((original.map(route), None, vec![]));
    };
    let birth = decode::<crate::store::entities::pin_lease_requirement::contract::BirthContract>(
        value(row, "contract").ok_or(SnapshotError::InvalidEvidence)?,
        work,
    )?;
    if !matches!(birth.provider_mode.as_str(), "one" | "all")
        || birth.provider_mode
            != value(
                frame
                    .row("pin_leases", &[&lease.id])
                    .ok_or(SnapshotError::InvalidEvidence)?,
                "provider_mode",
            )
            .unwrap_or("")
        || birth.candidate_order.is_empty()
        || birth.candidate_order.len() != birth.candidates.len()
        || birth.candidate_order.iter().collect::<BTreeSet<_>>().len()
            != birth.candidate_order.len()
        || value(row, "owner_object_id") != Some(&lease.object_id)
        || !birth
            .content_cids
            .iter()
            .any(|cid| canonical(cid).ok().as_deref() == Some(&target.cid))
    {
        return Err(SnapshotError::InvalidEvidence);
    }
    let key = [("lease_id".into(), lease.id.clone())].into();
    let mut candidates = Vec::new();
    for slot in &birth.candidate_order {
        let original = birth
            .candidates
            .get(slot)
            .ok_or(SnapshotError::InvalidEvidence)?
            .as_ref();
        let point =
            super::super::legacy::resolve(frame, "pin_lease_requirements", &key, slot, work)?;
        let (allocation, original, receipt_id) = if let Some(point) = point {
            // A declaration route alone is not historical account capability.
            let captured = point
                .historical_route
                .filter(|route| Some(route) == original);
            (point.operational_key, captured, Some(point.receipt_id))
        } else {
            (slot.clone(), original.cloned(), None)
        };
        candidates.push(FrozenCandidate {
            slot: slot.clone(),
            allocation,
            route: original.as_ref().map(route),
            receipt_id,
        });
    }
    let selected: Vec<_> = candidates
        .iter()
        .filter(|candidate| candidate.allocation == target.allocation)
        .collect();
    if selected.len() > 1 {
        return Err(SnapshotError::InvalidEvidence);
    }
    let selected = selected
        .first()
        .and_then(|candidate| candidate.route.clone());
    let requirements = selected.as_ref().and_then(|selected| {
        // One can act on its known chosen original route without inventing an
        // identity for another unknown slot. All keeps the stronger closure.
        // The authoritative full order (including None) is frozen_candidates,
        // never this projection of known IDs and never current availability.
        if birth.provider_mode == "all"
            && candidates.iter().any(|candidate| candidate.route.is_none())
        {
            return None;
        }
        let provider_ids = candidates
            .iter()
            .filter_map(|candidate| {
                candidate
                    .route
                    .as_ref()
                    .map(|route| route.provider_id.clone())
            })
            .collect::<Vec<_>>();
        if provider_ids.iter().collect::<BTreeSet<_>>().len() != provider_ids.len() {
            return None;
        }
        let original = selected.original.as_ref()?;
        let (min_replicas, max_age_secs, level) = match original.confirmation {
            Some(ConfirmationRequirement::ClusterReplicas {
                min_replicas,
                max_age_secs,
            }) if min_replicas > 0 && max_age_secs > 0 => (
                min_replicas,
                Some(max_age_secs),
                ConfirmationLevel::ReplicationComplete,
            ),
            Some(_) => return None,
            None if original.resource_type() != RemoteResourceType::ClusterPin => {
                (1, None, ConfirmationLevel::RemotePinned)
            }
            None => return None,
        };
        Some(Requirements {
            mode: if birth.provider_mode == "all" {
                ProviderMode::All
            } else {
                ProviderMode::One
            },
            provider_ids,
            levels: vec![level],
            min_replicas,
            max_age_secs,
            policy_revision: lease.policy_id.clone(),
        })
    });
    Ok((selected, requirements, candidates))
}
