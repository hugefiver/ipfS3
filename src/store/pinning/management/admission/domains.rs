use crate::{
    error::AppResult, pinning::identity::ProviderRouteSnapshot, store::entities::pin_provider_route,
};
use sea_orm::{ConnectionTrait, Statement};

pub(crate) fn allocation_domain(route: &ProviderRouteSnapshot) -> AppResult<String> {
    validate_domain(route)?;
    let tuple = serde_json::to_string(&(&route.backend, &route.scope, route.resource_type()))
        .map_err(|_| super::needs_attention())?;
    Ok(format!("domain:{tuple}"))
}
fn validate_domain(route: &ProviderRouteSnapshot) -> AppResult<()> {
    if route.backend.is_empty()
        || route.scope.is_empty()
        || route.storage_domain.is_empty()
        || route.provider_id.is_empty()
        || route.credential_revision == 0
        || route.endpoint_revision == 0
        || !matches!(
            route.api_profile.as_str(),
            "noop"
                | "psa"
                | "pinata-psa"
                | "filebase-psa"
                | "pinata-v3"
                | "pinata-legacy"
                | "rpc"
                | "kubo"
                | "filebase-rpc"
                | "cluster"
                | "cluster-rest"
                | "cluster-proxy"
        )
    {
        return Err(super::needs_attention());
    }
    Ok(())
}

pub(super) fn allocation_domain_with_work(
    route: &ProviderRouteSnapshot,
    work: super::DecodeWork<'_>,
) -> AppResult<String> {
    validate_domain(route)?;
    struct Length(usize);
    impl std::io::Write for Length {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("domain length"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let tuple = (&route.backend, &route.scope, route.resource_type());
    let mut length = Length(7);
    serde_json::to_writer(&mut length, &tuple).map_err(|_| super::needs_attention())?;
    work.index_cells(1, length.0).map_err(super::work_error)?;
    let mut output = Vec::with_capacity(length.0);
    output.extend_from_slice(b"domain:");
    serde_json::to_writer(&mut output, &tuple).map_err(|_| super::needs_attention())?;
    String::from_utf8(output).map_err(|_| super::needs_attention())
}
pub(super) fn registration_key_with_work(
    route: &ProviderRouteSnapshot,
    work: super::DecodeWork<'_>,
) -> AppResult<String> {
    match route.provider_id.strip_prefix("legacy:") {
        Some(alias) if !alias.is_empty() => {
            work.copy(alias).map_err(super::work_error)?;
            Ok(alias.into())
        }
        Some(_) => Err(super::needs_attention()),
        None => allocation_domain_with_work(route, work),
    }
}

pub(super) async fn registered_routes<C: ConnectionTrait>(
    db: &C,
    provider: &str,
) -> AppResult<Vec<ProviderRouteSnapshot>> {
    let schema = super::locks::AdmissionSchema::read(db).await?;
    let routes = registered_routes_in_schema(db, provider).await?;
    schema.revalidate(db).await?;
    Ok(routes)
}

pub(super) async fn registered_routes_in_schema<C: ConnectionTrait>(
    db: &C,
    provider: &str,
) -> AppResult<Vec<ProviderRouteSnapshot>> {
    let work = super::SharedDecodeBudget::default();
    registered_routes_in_schema_with_work(db, provider, super::DecodeWork::shared(&work)).await
}
pub(super) async fn registered_routes_in_schema_with_work<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    work: super::DecodeWork<'_>,
) -> AppResult<Vec<ProviderRouteSnapshot>> {
    work.with(|work| work.nodes(1)).map_err(super::work_error)?;
    let Some(row) = super::super::records::current_model::<pin_provider_route::Entity, _>(
        db,
        &[provider],
        work,
    )
    .await
    .map_err(super::work_error)?
    else {
        return Ok(Vec::new());
    };
    let route: ProviderRouteSnapshot = work
        .with(|work| work.typed(&row.snapshot))
        .map_err(super::work_error)?;
    if provider.starts_with("domain:") && allocation_domain_with_work(&route, work)? != provider {
        return Err(super::needs_attention());
    }
    Ok(vec![route])
}

pub(crate) async fn unregistered_legacy<C: ConnectionTrait>(
    db: &C,
    provider: &str,
) -> AppResult<bool> {
    let schema = super::locks::AdmissionSchema::read(db).await?;
    let unregistered = unregistered_legacy_in_schema(db, provider, &schema).await?;
    schema.revalidate(db).await?;
    Ok(unregistered)
}

pub(super) async fn unregistered_legacy_in_schema<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    schema: &super::locks::AdmissionSchema,
) -> AppResult<bool> {
    if provider.starts_with("domain:") {
        return Ok(false);
    }
    let sql = if schema.management_present() {
        "SELECT 1 AS present WHERE EXISTS(SELECT 1 FROM pin_provider_routes) OR EXISTS(SELECT 1 FROM pin_management_retirements)"
    } else {
        "SELECT 1 AS present WHERE EXISTS(SELECT 1 FROM pin_provider_routes)"
    };
    let row = db
        .query_one(Statement::from_string(db.get_database_backend(), sql))
        .await
        .map_err(|_| super::needs_attention())?;
    Ok(row.is_none())
}
