use super::model::*;
use crate::store::pinning::management::native::value;

pub(crate) fn logically_active(reference: &NativeRow) -> bool {
    matches!(reference.get("logical_released_at"), Some(None))
}

pub(crate) fn project(frame: &NativeFrame) -> Vec<RootReferenceSnapshot> {
    frame
        .frontier
        .iter()
        .filter(|key| key.table == "zip_root_references")
        .filter_map(|key| {
            let [batch, revision, epoch, node, tier, cid] = key.values.as_slice() else {
                return None;
            };
            let reference = frame.row(&key.table, &[batch, revision, epoch, node, tier, cid])?;
            let build = frame.row("zip_root_builds", &[batch, revision, epoch])?;
            let batch_tuple = frame.row("zip_batches", &[batch])?;
            Some(RootReferenceSnapshot {
                key: key.clone(),
                owner: value(batch_tuple, "owner")?.to_owned(),
                reference_tuple: reference.clone(),
                build_tuple: build.clone(),
                batch_tuple: batch_tuple.clone(),
                verification_recorded: frame
                    .original_value(
                        &key.table,
                        &[batch, revision, epoch, node, tier, cid],
                        "verification_receipt",
                    )
                    .is_some_and(|receipt| !receipt.is_empty()),
            })
        })
        .collect()
}

pub(crate) fn selected(
    request: &ItemRequest,
    snapshot: &super::SnapshotView<'_>,
) -> Result<Vec<RootReferenceSnapshot>, SafeError> {
    let Action::RetireWith {
        selection: RetirementSelection::ReleaseRootReferences { references },
    } = &request.action
    else {
        return Ok(vec![]);
    };
    let frame = snapshot.rows.ok_or(SafeError::UnknownEvidence)?;
    let mut result = Vec::with_capacity(references.len());
    for key in references {
        let proof = snapshot
            .root_references
            .iter()
            .find(|proof| proof.key == *key)
            .ok_or(SafeError::UnknownEvidence)?;
        let [batch, revision, epoch, node, tier, cid] = key.values.as_slice() else {
            return Err(SafeError::InvalidInput);
        };
        if !frame.projected_row_matches(
            "zip_root_references",
            &[batch, revision, epoch, node, tier, cid],
            &proof.reference_tuple,
        ) || !frame.projected_row_matches(
            "zip_root_builds",
            &[batch, revision, epoch],
            &proof.build_tuple,
        ) || !frame.projected_row_matches("zip_batches", &[batch], &proof.batch_tuple)
            || value(&proof.batch_tuple, "owner") != Some(proof.owner.as_str())
        {
            return Err(SafeError::InconsistentSnapshot);
        }
        if !logically_active(&proof.reference_tuple)
            || proof.owner.is_empty()
            || !proof.verification_recorded
            || !matches!(
                value(&proof.reference_tuple, "state"),
                Some("adopted" | "retained")
            )
            || value(&proof.build_tuple, "status") != Some("verified")
        {
            return Err(SafeError::UnknownEvidence);
        }
        // Public imported projections can reproduce a proposal, never private
        // authorization. In-process planning must agree with the original receipt.
        if snapshot
            .native_frame
            .is_none_or(|frame| frame.has_private_material())
            && frame
                .original_value(
                    "zip_root_references",
                    &[batch, revision, epoch, node, tier, cid],
                    "verification_receipt",
                )
                .is_none_or(|receipt| receipt.is_empty())
        {
            return Err(SafeError::UnknownEvidence);
        }
        result.push(proof.clone());
    }
    result.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(result)
}
