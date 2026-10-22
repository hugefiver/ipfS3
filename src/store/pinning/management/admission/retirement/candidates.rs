//! Exact marker originals and complete plan-member candidates from the locator.
use crate::store::pinning::management::{ManagementError, Result, native, records};
use records::locator::{AuthoritySelector, AuthorityView};
use sea_orm::ConnectionTrait;

pub(super) async fn rows<C: ConnectionTrait>(
    db: &C,
    candidates: &AuthorityView,
    work: records::DecodeWork<'_>,
) -> Result<Vec<(native::NativeRow, native::NativeRow)>> {
    let markers = candidates
        .originals
        .iter()
        .filter(|(key, _)| key.table == "pin_management_retirements")
        .map(|(_, row)| row)
        .collect::<Vec<_>>();
    if markers.is_empty() {
        return Ok(Vec::new());
    }
    let selectors = markers
        .iter()
        .map(|row| {
            Ok(AuthoritySelector::RecordMembers(
                native::value(*row, "database_scope")
                    .ok_or(ManagementError::InvalidEvidence)?
                    .into(),
                native::value(*row, "plan_id")
                    .ok_or(ManagementError::InvalidEvidence)?
                    .into(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let members = super::super::authority::load(db, &selectors, work).await?;
    let mut rows = Vec::with_capacity(markers.len());
    for marker in markers {
        let key = native::NativeKey {
            table: "pin_management_records".into(),
            values: ["database_scope", "plan_id", "item_id"]
                .iter()
                .map(|column| {
                    native::value(marker, column)
                        .map(str::to_owned)
                        .ok_or(ManagementError::InvalidEvidence)
                })
                .chain(std::iter::once(Ok("action".into())))
                .collect::<Result<_>>()?,
        };
        let action = members
            .originals
            .get(&key)
            .ok_or(ManagementError::InvalidEvidence)?;
        work.copy(&(marker, action))?;
        rows.push((marker.clone(), action.clone()));
    }
    Ok(rows)
}

pub(super) fn routes(
    candidates: &AuthorityView,
    work: records::DecodeWork<'_>,
) -> Result<Vec<crate::store::entities::pin_provider_route::Model>> {
    candidates
        .originals
        .iter()
        .filter(|(key, _)| key.table == "pin_provider_routes")
        .map(|(_, row)| {
            work.copy(row)?;
            let text = |column| {
                native::value(row, column)
                    .map(str::to_owned)
                    .ok_or(ManagementError::InvalidEvidence)
            };
            Ok(crate::store::entities::pin_provider_route::Model {
                provider: text("provider")?,
                snapshot: text("snapshot")?,
                display_name: text("display_name")?,
                retired: match native::value(row, "retired") {
                    Some("1" | "true") => true,
                    Some("0" | "false") => false,
                    _ => return Err(ManagementError::InvalidEvidence),
                },
            })
        })
        .collect()
}
