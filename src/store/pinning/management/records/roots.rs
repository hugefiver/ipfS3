//! Exact selected logical references, distinct from physical cleanup authority.
use super::*;
use crate::pinning::management::{Action, Capture, RetirementSelection};

pub(crate) fn validate_rows(
    action: &CommittedAction,
    before: FrozenRows<'_>,
    work: DecodeWork<'_>,
) -> Result<()> {
    let Action::RetireWith {
        selection: RetirementSelection::ReleaseRootReferences { references },
    } = &action.receipt.binding.request.action
    else {
        return if action.root_releases.is_empty() {
            Ok(())
        } else {
            Err(ManagementError::Confirmation)
        };
    };
    before.validate_current()?;
    let Some(Capture::Provider(capture)) = &action.receipt.binding.capture else {
        return Err(ManagementError::Confirmation);
    };
    work.copy(references)?;
    let mut selected = references.clone();
    selected.sort();
    if selected.is_empty()
        || selected.windows(2).any(|keys| keys[0] == keys[1])
        || action.root_releases != selected
        || capture.root_references.len() != selected.len()
        || !action.receipt.intent_ids.is_empty()
        || !action.jobs.is_empty()
        || !action.receipt.job_ids.is_empty()
        || !action.targets.is_empty()
        || !action.migration_points.is_empty()
        || action.decision.is_some()
    {
        return Err(ManagementError::Confirmation);
    }
    for key in &selected {
        let [batch, revision, epoch, node, tier, cid] = key.values.as_slice() else {
            return Err(ManagementError::InvalidEvidence);
        };
        if key.table != "zip_root_references" {
            return Err(ManagementError::InvalidEvidence);
        }
        let mut proofs = capture
            .root_references
            .iter()
            .filter(|proof| proof.key == *key);
        let proof = proofs.next().ok_or(ManagementError::Confirmation)?;
        if proofs.next().is_some() {
            return Err(ManagementError::Confirmation);
        }
        if !before.projected_row_matches(
            "zip_root_references",
            &[batch, revision, epoch, node, tier, cid],
            &proof.reference_tuple,
        ) || !before.projected_row_matches(
            "zip_root_builds",
            &[batch, revision, epoch],
            &proof.build_tuple,
        ) || !before.projected_row_matches("zip_batches", &[batch], &proof.batch_tuple)
            || native::value(&proof.batch_tuple, "owner") != Some(proof.owner.as_str())
            || proof.owner.is_empty()
            || !matches!(proof.reference_tuple.get("logical_released_at"), Some(None))
            || !matches!(
                native::value(&proof.reference_tuple, "state"),
                Some("retained" | "adopted")
            )
            || native::value(&proof.build_tuple, "status") != Some("verified")
            || !proof.verification_recorded
            || before
                .original_value(
                    "zip_root_references",
                    &[batch, revision, epoch, node, tier, cid],
                    "verification_receipt",
                )
                .is_none_or(|receipt| receipt.is_empty())
        {
            return Err(ManagementError::InvalidEvidence);
        }
    }
    Ok(())
}

pub(super) async fn validate_effect<C: ConnectionTrait>(
    db: &C,
    action: &CommittedAction,
    before: FrozenRows<'_>,
    work: DecodeWork<'_>,
) -> Result<()> {
    for key in &action.root_releases {
        work.with(|work| work.nodes(1 + key.values.len()))?;
        let refs = key.values.iter().map(String::as_str).collect::<Vec<_>>();
        let mut current = super::current_row(db, &key.table, &refs, work).await?;
        let marker = current
            .get_mut("logical_released_at")
            .ok_or(ManagementError::InvalidEvidence)?
            .take()
            .ok_or(ManagementError::InvalidEvidence)?;
        native::parse_time(&marker)?;
        if !before.original_row_matches(&key.table, &refs, &current) {
            return Err(ManagementError::Changed);
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "roots/tests.rs"]
mod tests;
