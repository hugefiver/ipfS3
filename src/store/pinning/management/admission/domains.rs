use sea_orm::{ConnectionTrait, EntityTrait, Statement};
use crate::{error::AppResult, pinning::identity::{ProviderRouteSnapshot, RemoteResourceType}, store::entities::pin_provider_route};

pub(crate) fn allocation_domain(route: &ProviderRouteSnapshot) -> AppResult<String> {
    if route.backend.is_empty() || route.scope.is_empty() || route.storage_domain.is_empty()
        || route.provider_id.is_empty() || route.credential_revision == 0 || route.endpoint_revision == 0
        || !matches!(route.api_profile.as_str(), "noop" | "psa" | "pinata-psa" | "filebase-psa"
            | "pinata-v3" | "pinata-legacy" | "rpc" | "kubo" | "filebase-rpc"
            | "cluster" | "cluster-rest" | "cluster-proxy") {
        return Err(super::needs_attention());
    }
    let tuple = serde_json::to_string(&(&route.backend, &route.scope, route.resource_type()))
        .map_err(|_| super::needs_attention())?;
    Ok(format!("domain:{tuple}"))
}

pub(super) fn canonical_domain(key: &str) -> bool {
    key.strip_prefix("domain:").and_then(|raw| serde_json::from_str::<(String, String, RemoteResourceType)>(raw).ok())
        .is_some_and(|tuple| !tuple.0.is_empty() && !tuple.1.is_empty()
            && serde_json::to_string(&tuple).ok().as_deref() == key.strip_prefix("domain:"))
}

pub(super) fn registration_key(route: &ProviderRouteSnapshot) -> AppResult<String> {
    match route.provider_id.strip_prefix("legacy:") {
        Some(alias) if !alias.is_empty() => Ok(alias.into()),
        Some(_) => Err(super::needs_attention()),
        None => allocation_domain(route),
    }
}

pub(super) async fn registered_routes<C: ConnectionTrait>(db: &C, provider: &str) -> AppResult<Vec<ProviderRouteSnapshot>> {
    let schema = super::locks::AdmissionSchema::read(db).await?;
    let routes = registered_routes_in_schema(db, provider).await?;
    schema.revalidate(db).await?;
    Ok(routes)
}

pub(super) async fn registered_routes_in_schema<C: ConnectionTrait>(db: &C, provider: &str) -> AppResult<Vec<ProviderRouteSnapshot>> {
    let Some(row) = pin_provider_route::Entity::find_by_id(provider.to_owned()).one(db).await
        .map_err(|_| super::needs_attention())? else { return Ok(Vec::new()); };
    let route: ProviderRouteSnapshot = serde_json::from_str(&row.snapshot).map_err(|_| super::needs_attention())?;
    if provider.starts_with("domain:") && allocation_domain(&route)? != provider { return Err(super::needs_attention()); }
    Ok(vec![route])
}

pub(crate) async fn unregistered_legacy<C: ConnectionTrait>(db: &C, provider: &str) -> AppResult<bool> {
    let schema = super::locks::AdmissionSchema::read(db).await?;
    let unregistered = unregistered_legacy_in_schema(db, provider, &schema).await?;
    schema.revalidate(db).await?;
    Ok(unregistered)
}

pub(super) async fn unregistered_legacy_in_schema<C: ConnectionTrait>(db: &C, provider: &str, schema: &super::locks::AdmissionSchema) -> AppResult<bool> {
    if provider.starts_with("domain:") { return Ok(false); }
    let sql = if schema.management_present() {
        "SELECT 1 AS present WHERE EXISTS(SELECT 1 FROM pin_provider_routes) OR EXISTS(SELECT 1 FROM pin_management_retirements)"
    } else {
        "SELECT 1 AS present WHERE EXISTS(SELECT 1 FROM pin_provider_routes)"
    };
    let row = db.query_one(Statement::from_string(db.get_database_backend(), sql))
        .await.map_err(|_| super::needs_attention())?;
    Ok(row.is_none())
}
