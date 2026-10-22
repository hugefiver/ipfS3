//! Durable domain admission with operation-local original-row validation.
use super::super::native;
use crate::pinning::management::{Action, Capture, PlannedStep, RetirementSelection, Subject};
use crate::{
    error::AppResult, pinning::identity::ProviderRouteSnapshot, store::entities::pin_provider_route,
};
use sea_orm::ConnectionTrait;
use std::collections::BTreeSet;

mod candidates;

#[cfg(test)]
#[path = "../../../../../tests/support/stage7_retirement_barrier_schema.rs"]
mod schema_tests;

#[cfg(test)]
mod budget_tests;

pub(super) async fn closed<C: ConnectionTrait>(
    db: &C,
    routes: &[ProviderRouteSnapshot],
    context: super::DecodeWork<'_>,
) -> AppResult<bool> {
    if routes.is_empty() {
        return Ok(false);
    }
    let schema = super::locks::AdmissionSchema::read(db).await?;
    let closed = closed_in_schema(db, routes, &schema, false, context).await?;
    schema.revalidate(db).await?;
    Ok(closed)
}

pub(super) async fn closed_in_schema<C: ConnectionTrait>(
    db: &C,
    routes: &[ProviderRouteSnapshot],
    schema: &super::locks::AdmissionSchema,
    registration: bool,
    context: super::DecodeWork<'_>,
) -> AppResult<bool> {
    closed_with_role(db, routes, schema, registration, None, context).await
}

pub(super) async fn closed_r26<C: ConnectionTrait>(
    db: &C,
    admission: &crate::store::pinning::legacy_migration::R26Admission<'_>,
    context: super::DecodeWork<'_>,
) -> AppResult<bool> {
    let schema = super::locks::AdmissionSchema::read(db).await?;
    let closed = closed_with_role(
        db,
        admission.routes(),
        &schema,
        false,
        Some(admission),
        context,
    )
    .await?;
    schema.revalidate(db).await?;
    Ok(closed)
}

async fn closed_with_role<C: ConnectionTrait>(
    db: &C,
    routes: &[ProviderRouteSnapshot],
    schema: &super::locks::AdmissionSchema,
    registration: bool,
    preservation: Option<&crate::store::pinning::legacy_migration::R26Admission<'_>>,
    context: super::DecodeWork<'_>,
) -> AppResult<bool> {
    context
        .with(|work| work.nodes(2 + routes.len()))
        .map_err(super::work_error)?;
    let requested = routes
        .iter()
        .map(|route| super::domains::allocation_domain_with_work(route, context))
        .collect::<AppResult<BTreeSet<_>>>()?;
    let mut closed = false;
    let mut historical_domains = BTreeSet::new();
    if !schema.management_present() {
        return Err(super::needs_attention());
    }
    use super::super::records::locator::AuthoritySelector;
    let mut selectors = requested
        .iter()
        .cloned()
        .flat_map(|domain| {
            [
                AuthoritySelector::RegisteredDomain(domain.clone()),
                AuthoritySelector::RetirementDomain(domain),
            ]
        })
        .collect::<BTreeSet<_>>();
    if let Some(admission) = preservation {
        for source in admission.sources() {
            context.copy(source).map_err(super::work_error)?;
            selectors.insert(AuthoritySelector::RegisteredRoute(source.into()));
            selectors.insert(AuthoritySelector::RetirementDomain(source.into()));
        }
    }
    for route in routes {
        for name in [
            route.provider_id.clone(),
            super::domains::registration_key_with_work(route, context)?,
        ] {
            selectors.insert(AuthoritySelector::RegisteredRoute(name.clone()));
            selectors.insert(AuthoritySelector::RetirementDomain(name));
        }
        if let Some(alias) = route.provider_id.strip_prefix("legacy:") {
            selectors.insert(AuthoritySelector::RetirementDomain(alias.into()));
        }
    }
    let selected = super::authority::load(db, &selectors.into_iter().collect::<Vec<_>>(), context)
        .await
        .map_err(super::work_error)?;
    let registered = candidates::routes(&selected, context).map_err(super::work_error)?;
    context
        .with(|work| work.nodes(1 + registered.len()))
        .map_err(super::work_error)?;
    let mut parsed = Vec::new();
    for row in &registered {
        let original = context
            .with(|work| work.typed_shared::<ProviderRouteSnapshot>(&row.snapshot))
            .map_err(super::work_error)?;
        let domain = super::domains::allocation_domain_with_work(&original, context)?;
        if row.provider.starts_with("domain:") && row.provider != domain {
            return Err(super::needs_attention());
        }
        if row.retired
            && (requested.contains(&domain)
                || preservation.is_some_and(|admission| admission.references(&row.provider)))
        {
            closed = true;
        }
        parsed.push(original);
    }
    let retirements = candidates::rows(db, &selected, context)
        .await
        .map_err(super::work_error)?;
    if retirements.is_empty() {
        if registration && closed {
            return Ok(true);
        }
        validate_legacy_routes(
            routes,
            &registered,
            &parsed,
            &historical_domains,
            registration,
            preservation,
            context,
        )?;
        return Ok(closed);
    }
    context.index_cells(1, 64).map_err(super::work_error)?;
    let identity = super::super::native::database_identity(db)
        .await
        .map_err(super::work_error)?;
    let (scopes,bytes)=super::super::records::bounded_projection(db,"FROM pin_management_records WHERE kind='database_scope' AND state='identity' AND plan_id='schema' AND item_id='identity'",&[("database_scope","database_scope")],"database_scope",vec![],(1,native::MAX_BYTES),context).await.map_err(super::work_error)?;
    context
        .index_cells(scopes.len(), bytes)
        .map_err(super::work_error)?;
    let scope = scopes
        .first()
        .ok_or_else(super::needs_attention)?
        .try_get::<String>("", "database_scope")
        .map_err(|_| super::needs_attention())?;
    context
        .with(|work| work.nodes(1 + retirements.len()))
        .map_err(super::work_error)?;
    let plans = retirements
        .iter()
        .map(|(row, _)| {
            let plan = native::value(row, "plan_id").ok_or_else(super::needs_attention)?;
            context.copy(plan).map_err(super::work_error)?;
            Ok(plan.to_owned())
        })
        .collect::<AppResult<Vec<_>>>()?;
    let mut source = super::super::records::evidence::Evidence::load_roots_with_work(
        db,
        &scope,
        &identity,
        &plans,
        &retirements,
        context.budget().clone(),
    )
    .await
    .map_err(super::work_error)?;
    for (row, action) in &retirements {
        let text = |column: &str| native::value(row, column).ok_or_else(super::needs_attention);
        if text("database_scope")? != scope
            || text("native_identity")? != identity
            || identity.starts_with("diagnostic:")
        {
            return Err(super::needs_attention());
        }
        let key = text("allocation_domain")?;
        let receipt = super::super::records::classify_retirement_root(&source, row, action)
            .map_err(super::work_error)?;
        let (
            Subject::Provider { route },
            Some(Capture::Provider(capture)),
            Some(PlannedStep::Retire(step)),
        ) = (
            &receipt.binding.request.subject,
            &receipt.binding.capture,
            &receipt.binding.step,
        )
        else {
            return Err(super::needs_attention());
        };
        let original = route.original.as_ref().ok_or_else(super::needs_attention)?;
        let disposition = text("disposition")?;
        let selected = match &receipt.binding.request.action {
            Action::Retire
            | Action::RetireWith {
                selection:
                    RetirementSelection::Retain | RetirementSelection::ReleaseRootReferences { .. },
            } => "retain",
            Action::RetireWith {
                selection: RetirementSelection::Migrate { .. },
            } => "migrate",
            Action::RetireWith {
                selection: RetirementSelection::ControlledCleanup { .. },
            } => "controlled_cleanup",
            _ => return Err(super::needs_attention()),
        };
        if disposition != selected || !receipt.intent_ids.is_empty() || !step.stop_new_allocations {
            return Err(super::needs_attention());
        }
        let domain = super::domains::allocation_domain_with_work(original, context)?;
        if key.starts_with("domain:") && key != domain {
            return Err(super::needs_attention());
        }
        // Classification is a locator only. The records review must validate
        // the sealed inventory and its totals before this marker closes a
        // domain; mutation_scope is not the complete responsibility set.
        context.copy(text("plan_id")?).map_err(super::work_error)?;
        source
            .load_dependencies(db, &[text("plan_id")?.to_owned()])
            .await
            .map_err(super::work_error)?;
        validate_original_point(&source, key, original, row, action, context)?;
        if !key.starts_with("domain:") {
            // Resolve the old alias from its complete, DB-bound original
            // receipt/capture even after R26 moves today's registry row. The
            // key is only a locator; membership plus the full original route
            // supplies the backend/scope/resource-type relation.
            if !capture.mutation_scope.routes.iter().any(|captured| {
                captured.original.as_ref() == Some(original)
                    && captured.identity.original.as_ref() == Some(original)
            }) {
                return Err(super::needs_attention());
            }
            context.copy(&domain).map_err(super::work_error)?;
            historical_domains.insert(domain.clone());
        }
        if requested.contains(&domain) {
            context.copy(&domain).map_err(super::work_error)?;
            historical_domains.insert(domain.clone());
        }
        closed |= requested.contains(&domain)
            || preservation.is_some_and(|admission| admission.references(key));
    }
    validate_legacy_routes(
        routes,
        &registered,
        &parsed,
        &historical_domains,
        false,
        preservation,
        context,
    )?;
    Ok(closed)
}

fn validate_legacy_routes(
    routes: &[ProviderRouteSnapshot],
    registered: &[pin_provider_route::Model],
    parsed: &[std::sync::Arc<ProviderRouteSnapshot>],
    historical_domains: &BTreeSet<String>,
    registration: bool,
    preservation: Option<&crate::store::pinning::legacy_migration::R26Admission<'_>>,
    work: super::DecodeWork<'_>,
) -> AppResult<()> {
    // Removing explicit identity cannot prove a new account. A previously
    // registered original legacy route may still prove a different domain.
    for route in routes
        .iter()
        .filter(|route| route.provider_id.starts_with("legacy:"))
    {
        if historical_domains.contains(&super::domains::allocation_domain_with_work(route, work)?) {
            continue;
        }
        let key = super::domains::registration_key_with_work(route, work)?;
        // Only the exact validated R26 original may remain unknown. A conflicting
        // registered alias is still ambiguous and cannot be reinterpreted.
        if preservation.is_some_and(|admission| admission.preserves(route))
            && !registered.iter().any(|row| row.provider == key)
        {
            continue;
        }
        // A genuinely new metadata registration is not allocation admission.
        // Never reinterpret an already registered alias as another old account.
        if registration && !registered.iter().any(|row| row.provider == key) {
            continue;
        }
        if !registered
            .iter()
            .zip(parsed)
            .any(|(row, original)| row.provider == key && original.as_ref() == route)
        {
            return Err(super::needs_attention());
        }
    }
    Ok(())
}

fn validate_original_point(
    source: &super::super::records::evidence::Evidence,
    key: &str,
    original: &ProviderRouteSnapshot,
    row: &native::NativeRow,
    action: &native::NativeRow,
    work: super::super::records::DecodeWork<'_>,
) -> AppResult<()> {
    let review = super::super::records::retirement_review_from_source(source, row, action)
        .map_err(super::work_error)?;
    if review
        .rows()
        .row("pin_provider_routes", &[key])
        .and_then(|row| super::super::native::value(row, "snapshot"))
        .map(|snapshot| work.with(|work| work.typed::<ProviderRouteSnapshot>(snapshot)))
        .transpose()
        .map_err(super::work_error)?
        .as_ref()
        != Some(original)
    {
        return Err(super::needs_attention());
    }
    Ok(())
}
