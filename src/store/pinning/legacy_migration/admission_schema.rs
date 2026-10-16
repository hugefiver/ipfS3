//! Admission classifies the registered catalog without reading business images.
use super::{refused, snapshot};
use crate::{
    error::AppResult,
    store::pinning::management::{catalog, native},
};
use sea_orm::ConnectionTrait;

pub(crate) async fn admission_schema<C: ConnectionTrait>(db: &C) -> AppResult<Vec<String>> {
    let versions = snapshot::schema(db, false).await?;
    let route = native::registry()
        .into_iter()
        .find(|table| table.table == "pin_provider_routes")
        .expect("registered provider route catalog");
    catalog::validate_table(db, &route)
        .await
        .map_err(|_| refused("unknown, shadowed or incomplete provider route catalog"))?;
    Ok(versions)
}
