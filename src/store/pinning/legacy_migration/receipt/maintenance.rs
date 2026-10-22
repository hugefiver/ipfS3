//! Complete receipt frontier for explicit maintenance on a validated historical
//! catalog. Ordinary consumers require ready S75 locators and never use this path.
use super::*;

pub(super) async fn source_has_receipt<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    versions: &[String],
) -> AppResult<bool> {
    let ids = super::super::snapshot::frontier(db, versions).await?;
    if ids.is_empty() {
        return Ok(false);
    }
    budget(db, 0).await?;
    let mut active = false;
    for id in ids {
        let row = lookup(db, &id)
            .await?
            .ok_or_else(|| refused("receipt disappeared"))?;
        let confirmation: Confirmation = serde_json::from_str(&row.confirmation)
            .map_err(|_| refused("invalid DB receipt confirmation"))?;
        let declaration: super::super::Declaration = serde_json::from_str(&row.declaration)
            .map_err(|_| refused("invalid DB receipt declaration"))?;
        if row.schema_version != SCHEMA
            || row.database_scope != confirmation.database_scope
            || declaration != confirmation.declaration
        {
            return Err(refused("receipt header differs from its confirmation"));
        }
        match row.direction.as_str() {
            "apply" if row.reverses.is_none() => {
                matches(&row, &confirmation)?;
                if reverse(db, &row.id).await?.is_none()
                    && declaration
                        .namespaces
                        .iter()
                        .any(|ns| ns.source == provider)
                {
                    active = true;
                }
            }
            "rollback" => {
                let original = lookup(
                    db,
                    row.reverses
                        .as_deref()
                        .ok_or_else(|| refused("rollback original missing"))?,
                )
                .await?
                .ok_or_else(|| refused("rollback original missing"))?;
                if original.direction != "apply" || original.reverses.is_some() {
                    return Err(refused("invalid rollback original"));
                }
                rollback_matches(&original, &confirmation)?;
            }
            _ => return Err(refused("invalid receipt direction")),
        }
    }
    Ok(active)
}
