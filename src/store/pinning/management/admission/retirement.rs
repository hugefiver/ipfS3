//! Narrow durable domain index: no native::capture or global inventory budget.
use std::collections::BTreeSet;
use sea_orm::{ConnectionTrait, DatabaseBackend, EntityTrait, Statement};
use crate::{error::AppResult, pinning::identity::ProviderRouteSnapshot, store::entities::pin_provider_route};
use crate::pinning::management::{Action, Capture, PlannedStep, Subject, RetirementSelection};

mod index;

#[cfg(test)]
#[path = "../../../../../tests/support/stage7_retirement_barrier_schema.rs"]
mod schema_tests;

pub(super) async fn closed<C: ConnectionTrait>(db: &C, routes: &[ProviderRouteSnapshot]) -> AppResult<bool> {
    if routes.is_empty() { return Ok(false); }
    let schema = super::locks::AdmissionSchema::read(db).await?;
    let closed = closed_in_schema(db, routes, &schema, false).await?;
    schema.revalidate(db).await?;
    Ok(closed)
}

pub(super) async fn closed_in_schema<C: ConnectionTrait>(db: &C, routes: &[ProviderRouteSnapshot], schema: &super::locks::AdmissionSchema, registration: bool) -> AppResult<bool> {
    let requested = routes.iter().map(super::allocation_domain).collect::<AppResult<BTreeSet<_>>>()?;
    let mut closed = false;
    let mut historical_domains = BTreeSet::new();
    // Registry snapshots, never current config, resolve an old alias. Compare
    // the complete resource family, not owns_allocation_domain's backend/scope.
    let registered = pin_provider_route::Entity::find().all(db).await.map_err(|_| super::needs_attention())?;
    for row in &registered {
        let original: ProviderRouteSnapshot = serde_json::from_str(&row.snapshot).map_err(|_| super::needs_attention())?;
        let domain = super::allocation_domain(&original)?;
        if row.provider.starts_with("domain:") && row.provider != domain { return Err(super::needs_attention()); }
        if row.retired && requested.contains(&domain) { closed = true; }
    }
    if !schema.management_present() {
        // A closed native domain only tells registration to preserve history;
        // it cannot add the new alias or allocate using its changed transport.
        if registration && closed { return Ok(true); }
        validate_legacy_routes(routes, &registered, &historical_domains, registration)?;
        return Ok(closed);
    }
    let sql = match db.get_database_backend() {
        DatabaseBackend::Sqlite => index::SQLITE_ROUTES,
        DatabaseBackend::Postgres => index::POSTGRES_ROUTES,
        _ => return Err(super::needs_attention()),
    };
    let retirements = db.query_all(Statement::from_string(db.get_database_backend(), sql))
        .await.map_err(|_| super::needs_attention())?;
    if retirements.is_empty() {
        if registration && closed { return Ok(true); }
        validate_legacy_routes(routes, &registered, &historical_domains, registration)?;
        return Ok(closed);
    }
    let identity = super::super::native::database_identity(db).await.map_err(|_| super::needs_attention())?;
    let scope = db.query_one(Statement::from_string(db.get_database_backend(),
        "SELECT database_scope FROM pin_management_records WHERE kind='database_scope' AND state='identity' AND plan_id='schema' AND item_id='identity'"))
        .await.map_err(|_| super::needs_attention())?.ok_or_else(super::needs_attention)?
        .try_get::<String>("", "database_scope").map_err(|_| super::needs_attention())?;
    for row in retirements {
        let text = |column: &str| row.try_get::<String>("", column).map_err(|_| super::needs_attention());
        if text("database_scope")? != scope || text("native_identity")? != identity || identity.starts_with("diagnostic:")
            || row.try_get::<i64>("", "sealed").map_err(|_| super::needs_attention())? != 1 {
            return Err(super::needs_attention());
        }
        let key = text("allocation_domain")?;
        let (receipt, _) = super::super::records::decode_action_result(&text("receipt")?)
            .map_err(|_| super::needs_attention())?;
        let (Subject::Provider { route }, Some(Capture::Provider(capture)), Some(PlannedStep::Retire(step))) =
            (&receipt.binding.request.subject, &receipt.binding.capture, &receipt.binding.step) else { return Err(super::needs_attention()); };
        let original = route.original.as_ref().ok_or_else(super::needs_attention)?;
        let disposition = text("disposition")?;
        let selected = match &receipt.binding.request.action {
            Action::Retire | Action::RetireWith { selection: RetirementSelection::Retain } => "retain",
            Action::RetireWith { selection: RetirementSelection::Migrate { .. } } => "migrate",
            Action::RetireWith { selection: RetirementSelection::ControlledCleanup { .. } } => "controlled_cleanup",
            _ => return Err(super::needs_attention()),
        };
        if disposition != selected || !receipt.intent_ids.is_empty()
            || (selected != "controlled_cleanup" && !receipt.job_ids.is_empty())
            || !capture.inventory.complete || !step.stop_new_allocations {
            return Err(super::needs_attention());
        }
        let domain = super::allocation_domain(original)?;
        if key.starts_with("domain:") {
            if !super::domains::canonical_domain(&key) || key != domain { return Err(super::needs_attention()); }
        }
        if !key.starts_with("domain:") {
            // Resolve the old alias from its complete, DB-bound original
            // receipt/capture even after R26 moves today's registry row. The
            // key is only a locator; membership plus the full original route
            // supplies the backend/scope/resource-type relation.
            if !capture.inventory.frontier.iter().any(|point|
                point.table == "pin_provider_routes" && point.values == [key.clone()])
                || !capture.routes.iter().any(|captured| captured.original.as_ref() == Some(original)
                    && captured.identity.original.as_ref() == Some(original)) {
                return Err(super::needs_attention());
            }
            historical_domains.insert(domain.clone());
        }
        if requested.contains(&domain) {
            // Only a matching closed-domain point needs the retained full-frame
            // recheck. Open births do not scan each legacy private inventory.
            validate_original_point(db, &key, original).await?;
            historical_domains.insert(domain.clone());
        }
        closed |= requested.contains(&domain);
    }
    validate_legacy_routes(routes, &registered, &historical_domains, false)?;
    Ok(closed)
}

fn validate_legacy_routes(routes: &[ProviderRouteSnapshot], registered: &[pin_provider_route::Model], historical_domains: &BTreeSet<String>, registration: bool) -> AppResult<()> {
    // Removing explicit identity cannot prove a new account. A previously
    // registered original legacy route may still prove a different domain.
    for route in routes.iter().filter(|route| route.provider_id.starts_with("legacy:")) {
        if historical_domains.contains(&super::allocation_domain(route)?) { continue; }
        let key = super::domains::registration_key(route)?;
        // A genuinely new metadata registration is not allocation admission.
        // Never reinterpret an already registered alias as another old account.
        if registration && !registered.iter().any(|row| row.provider == key) { continue; }
        if !registered.iter().any(|row| row.provider == key
            && serde_json::from_str::<ProviderRouteSnapshot>(&row.snapshot).ok().as_ref() == Some(route)) {
            return Err(super::needs_attention());
        }
    }
    Ok(())
}

async fn validate_original_point<C: ConnectionTrait>(db: &C, key: &str, original: &ProviderRouteSnapshot) -> AppResult<()> {
    let sql = if db.get_database_backend() == DatabaseBackend::Sqlite { index::SQLITE } else { index::POSTGRES };
    let proof = db.query_one(Statement::from_sql_and_values(db.get_database_backend(), sql, [key.into()]))
        .await.map_err(|_| super::needs_attention())?.ok_or_else(super::needs_attention)?;
    if proof.try_get::<i64>("", "sealed").map_err(|_| super::needs_attention())? != 1 { return Err(super::needs_attention()); }
    // Matching closed points also use the record owner's lossless strict codec,
    // including packed snapshot/private strings. No SQL-extracted packed string
    // is treated as a raw route and no codec or authority is reimplemented.
    let columns = ["allocation_domain", "database_scope", "plan_id", "item_id", "principal", "native_identity",
        "binding", "inventory", "disposition", "unresolved", "state", "created_at", "record_kind"];
    let fields = columns.iter().map(|column| format!("CAST(\"{column}\" AS TEXT) AS \"{column}\""))
        .collect::<Vec<_>>().join(",");
    let row = db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
        format!("SELECT {fields} FROM pin_management_retirements WHERE allocation_domain=$1"), [key.into()]))
        .await.map_err(|_| super::needs_attention())?.ok_or_else(super::needs_attention)?;
    let mut native = super::super::native::NativeRow::new();
    for column in columns { native.insert(column.into(), row.try_get::<Option<String>>("", column).map_err(|_| super::needs_attention())?); }
    let super::super::records::RecordPayload::Retirement { inventory, .. } =
        super::super::records::inspect_original_row("pin_management_retirements", &native).map_err(|_| super::needs_attention())?
        else { return Err(super::needs_attention()); };
    if inventory.row("pin_provider_routes", &[key]).and_then(|row| super::super::native::value(row, "snapshot"))
        .and_then(|snapshot| serde_json::from_str::<ProviderRouteSnapshot>(snapshot).ok()).as_ref() != Some(original)
    { return Err(super::needs_attention()); }
    Ok(())
}
