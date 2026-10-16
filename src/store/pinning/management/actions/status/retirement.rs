//! A retirement barrier is durable; its outstanding responsibility is current.
use super::*;
use crate::pinning::{
    identity::ProviderRouteSnapshot,
    management::{RetirementSelection, Subject},
};

pub(super) async fn current_state<C: ConnectionTrait>(
    db: &C,
    action: &records::CommittedAction,
    work: records::SharedDecodeBudget,
) -> Result<records::ActionState> {
    use records::ActionState::*;
    let Subject::Provider { route } = &action.receipt.binding.request.subject else {
        return Err(ManagementError::InvalidEvidence);
    };
    let original = route
        .original
        .as_ref()
        .ok_or(ManagementError::InvalidEvidence)?;
    let context = records::DecodeWork::shared(&work);
    let current = records::CurrentRows::capture(db, context).await?;
    let frame = records::FrozenRows::Current(&current);
    let registry = frame
        .table("pin_provider_routes")
        .ok_or(ManagementError::Schema)?;
    context.with(|work| work.nodes(1 + registry.rows.iter().count()))?;
    let mut domains = Vec::new();
    for row in registry.rows.iter() {
        let route: ProviderRouteSnapshot = context.with(|work| {
            work.typed(
                row.cell("snapshot")
                    .ok_or(ManagementError::InvalidEvidence)?,
            )
        })?;
        if same_domain(&route, original) {
            domains.push(row);
        }
    }
    if domains.is_empty()
        || domains
            .iter()
            .any(|row| !matches!(row.cell("retired"), Some("1" | "true")))
    {
        return Ok(NeedsAttention);
    }
    if frame
        .table("zip_root_references")
        .ok_or(ManagementError::Schema)?
        .rows
        .iter()
        .any(|row| row.cell("logical_released_at").is_none())
    {
        return Ok(NeedsAttention);
    }
    let mut queued = false;
    for table in ["remote_pin_ledger", "pin_resource_history"] {
        for row in frame
            .table(table)
            .ok_or(ManagementError::Schema)?
            .rows
            .iter()
        {
            let (resource_route, effect, error, absent) = if table == "remote_pin_ledger" {
                context.copy(&row.cell("effect"))?;
                (
                    row.cell("route")
                        .map(|text| context.with(|work| work.typed::<ProviderRouteSnapshot>(text)))
                        .transpose()?,
                    row.cell("effect").map(str::to_owned),
                    row.cell("last_error").is_some(),
                    row.cell("effect") == Some("absent"),
                )
            } else {
                let envelope: History = context.with(|work| {
                    work.typed(row.cell("ledger").ok_or(ManagementError::InvalidEvidence)?)
                })?;
                (
                    envelope
                        .ledger
                        .route
                        .as_deref()
                        .map(|text| context.with(|work| work.typed::<ProviderRouteSnapshot>(text)))
                        .transpose()?,
                    envelope.ledger.effect,
                    envelope.ledger.last_error.is_some(),
                    envelope.confirmed_absent,
                )
            };
            let Some(resource_route) = resource_route else {
                return Ok(NeedsAttention);
            };
            if !same_domain(&resource_route, original) || absent {
                continue;
            }
            if error
                || matches!(
                    effect.as_deref(),
                    None | Some("reserved" | "unknown" | "reported_unknown" | "cleanup_pending")
                )
                || table == "pin_resource_history"
            {
                return Ok(NeedsAttention);
            }
        }
    }
    for job in frame
        .table("pin_jobs")
        .ok_or(ManagementError::Schema)?
        .rows
        .iter()
    {
        if !domains
            .iter()
            .any(|row| row.cell("provider") == job.cell("provider"))
            || job.cell("state") == Some("done")
        {
            continue;
        }
        if job.cell("last_error").is_some()
            || job.cell("state") == Some("running") && job.cell("locked_until").is_none()
        {
            return Ok(NeedsAttention);
        }
        if job.cell("operation") == Some("unpin") || job.cell("state") == Some("running") {
            queued = true;
        }
    }
    if let Action::RetireWith {
        selection: RetirementSelection::Migrate { .. },
    } = &action.receipt.binding.request.action
    {
        let now = crate::store::database_clock::database_now(db)
            .await
            .map_err(|_| ManagementError::Database)?;
        for id in &action.targets {
            let Some(target) = frame.row("pin_lease_targets", &[id]) else {
                return Ok(NeedsAttention);
            };
            let text = |column| target.cell(column).ok_or(ManagementError::InvalidEvidence);
            let Some(lease) = frame.row("pin_leases", &[text("lease_id")?]) else {
                return Ok(NeedsAttention);
            };
            if target.cell("state") != Some("pinned")
                || lease.cell("state") != Some("active")
                || native::parse_time(
                    lease
                        .cell("expires_at")
                        .ok_or(ManagementError::InvalidEvidence)?,
                )? <= now
            {
                return Ok(NeedsAttention);
            }
            let Some(remote) = frame.row("remote_pins", &[text("provider")?, text("cid")?]) else {
                return Ok(NeedsAttention);
            };
            let Some(ledger) = frame.row("remote_pin_ledger", &[text("provider")?, text("cid")?])
            else {
                return Ok(NeedsAttention);
            };
            let route: ProviderRouteSnapshot = context.with(|work| {
                work.typed(
                    ledger
                        .cell("route")
                        .ok_or(ManagementError::InvalidEvidence)?,
                )
            })?;
            if remote.cell("status") != Some("pinned")
                && !(route.resource_type() == RemoteResourceType::ClusterPin
                    && remote.cell("status") == Some("pinning"))
            {
                return Ok(NeedsAttention);
            }
            if !pin_lease_requirement::contract::target_confirmed_with_work(
                db,
                text("lease_id")?,
                text("provider")?,
                text("cid")?,
                now,
                work.clone(),
            )
            .await?
            {
                return Ok(NeedsAttention);
            }
        }
    }
    Ok(if queued { Queued } else { Accepted })
}
#[derive(serde::Deserialize)]
struct History {
    ledger: Ledger,
    #[serde(default)]
    confirmed_absent: bool,
}
#[derive(serde::Deserialize)]
struct Ledger {
    route: Option<String>,
    effect: Option<String>,
    last_error: Option<String>,
}
fn same_domain(left: &ProviderRouteSnapshot, right: &ProviderRouteSnapshot) -> bool {
    left.backend == right.backend
        && left.scope == right.scope
        && left.resource_type() == right.resource_type()
}
