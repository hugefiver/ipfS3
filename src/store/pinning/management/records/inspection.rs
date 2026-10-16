//! Typed read-only inspection over the operation's original rows and review arena.
use super::*;

pub(crate) async fn inspect_review_with_work<C: ConnectionTrait>(
    db: &C,
    row: &native::NativeRow,
    work: SharedDecodeBudget,
) -> Result<archive::ReviewHandle> {
    let scope = native::value(row, "database_scope").ok_or(ManagementError::InvalidEvidence)?;
    let identity = native::value(row, "native_identity").ok_or(ManagementError::InvalidEvidence)?;
    let plan = native::value(row, "plan_id").ok_or(ManagementError::InvalidEvidence)?;
    DecodeWork::shared(&work).copy(plan)?;
    let source =
        evidence::Evidence::load_with_work(db, scope, identity, &[plan.into()], work).await?;
    if source.row(&evidence::key(row)?)? != row {
        return Err(ManagementError::Changed);
    }
    let result = archive::load_review(&source, row, &mut *source.budget()?)?;
    if !matches!(native::value(row, "state"), Some("prepared" | "applied")) {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(result)
}
pub(crate) async fn inspect_retirement_review_with_work<C: ConnectionTrait>(
    db: &C,
    row: &native::NativeRow,
    work: SharedDecodeBudget,
) -> Result<archive::ReviewHandle> {
    let scope = native::value(row, "database_scope").ok_or(ManagementError::InvalidEvidence)?;
    let identity = native::value(row, "native_identity").ok_or(ManagementError::InvalidEvidence)?;
    let plan = native::value(row, "plan_id").ok_or(ManagementError::InvalidEvidence)?;
    DecodeWork::shared(&work).copy(plan)?;
    let source =
        evidence::Evidence::load_with_work(db, scope, identity, &[plan.into()], work).await?;
    let action = source.retirement_action(row)?;
    retirement::retirement_review_from_source(&source, row, action)
}

pub(crate) enum RecordPayload {
    Identity,
    Evidence {
        attestation: Box<crate::pinning::management::evidence::ReadAttestation>,
        original_object: native::NativeRow,
        original_version: native::NativeRow,
    },
}
pub(crate) fn inspect_original_row(table: &str, row: &native::NativeRow) -> Result<RecordPayload> {
    inspect_original_row_with_work(
        table,
        row,
        DecodeWork::shared(&SharedDecodeBudget::default()),
    )
}
pub(crate) fn inspect_original_row_with_work(
    table: &str,
    row: &native::NativeRow,
    work: DecodeWork<'_>,
) -> Result<RecordPayload> {
    if table != super::super::schema::RECORDS {
        return Err(ManagementError::InvalidEvidence);
    }
    match native::value(row, "kind") {
        Some("database_scope") => {
            if native::value(row, "state") == Some("identity") {
                Ok(RecordPayload::Identity)
            } else {
                Err(ManagementError::InvalidEvidence)
            }
        }
        Some("evidence_preparation") => {
            let (attestation, _endpoint): (
                crate::pinning::management::evidence::ReadAttestation,
                String,
            ) = work.with(|work| {
                work.typed(native::value(row, "result").ok_or(ManagementError::InvalidEvidence)?)
            })?;
            let (original_object, original_version): (native::NativeRow, native::NativeRow) = work
                .with(|work| {
                    work.typed(
                        native::value(row, "native_before")
                            .ok_or(ManagementError::InvalidEvidence)?,
                    )
                })?;
            if native::value(row, "state") != Some("prepared")
                || native::value(row, "completed_at").is_none()
                || native::value(row, "item_id") != Some(&attestation.identity.private_version_id)
                || native::value(&original_object, "id") != Some(&attestation.identity.object_id)
                || native::value(&original_version, "id")
                    != Some(&attestation.identity.private_version_id)
                || native::value(&original_version, "object_id")
                    != Some(&attestation.identity.object_id)
            {
                return Err(ManagementError::Confirmation);
            }
            work.with(|work| work.nodes(1))?;
            Ok(RecordPayload::Evidence {
                attestation: Box::new(attestation),
                original_object,
                original_version,
            })
        }
        _ => Err(ManagementError::InvalidEvidence),
    }
}
