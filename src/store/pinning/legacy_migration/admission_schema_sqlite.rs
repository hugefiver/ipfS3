//! One classification read for versions and bounded catalog-presence facts.
use super::{catalog, native, refused, snapshot};
use crate::error::AppResult;
use sea_orm::{ConnectionTrait, DatabaseBackend, QueryResult, Statement};

// The version branch retains its original 65-row bound and String decoding.
// The other branch returns only fixed-size EXISTS results, never DDL or cells.
const METADATA: &str = "SELECT 0 AS kind,version,\
    NULL AS management_shadow,NULL AS management_objects,NULL AS receipt_objects,\
    NULL AS route_shadow,NULL AS route_main,NULL AS receipt_shadow,NULL AS receipt_partial \
    FROM (SELECT version FROM seaql_migrations ORDER BY version LIMIT 65) \
    UNION ALL SELECT 1,NULL,\
    EXISTS(SELECT 1 FROM sqlite_temp_schema WHERE name IN ('pin_management_records','pin_management_retirements','seaql_migrations')),\
    EXISTS(SELECT 1 FROM sqlite_schema WHERE name IN ('pin_management_records','pin_management_retirements') OR tbl_name IN ('pin_management_records','pin_management_retirements')),\
    EXISTS(SELECT 1 FROM sqlite_schema WHERE type IN ('table','view') AND name IN ('pin_legacy_migrations','pin_legacy_migration_items')),\
    EXISTS(SELECT 1 FROM sqlite_temp_schema WHERE name IN ('seaql_migrations','pin_provider_routes') AND type IN ('table','view')),\
    EXISTS(SELECT 1 FROM pragma_table_list WHERE schema='main' AND name='pin_provider_routes' AND type='table'),\
    EXISTS(SELECT 1 FROM sqlite_temp_schema WHERE name IN ('pin_legacy_migrations','pin_legacy_migration_items') OR name GLOB 'pin_management_*' OR tbl_name GLOB 'pin_management_*'),\
    EXISTS(SELECT 1 FROM sqlite_schema WHERE name GLOB 'pin_management_*' OR tbl_name GLOB 'pin_management_*' OR name GLOB 's75_*') \
    ORDER BY kind,version";

fn exists(row: &QueryResult, column: &str) -> AppResult<bool> {
    Ok(row.try_get::<i32>("", column)? != 0)
}

pub(super) async fn classify<C: ConnectionTrait>(db: &C, receipt: bool) -> AppResult<Vec<String>> {
    let mut rows = db
        .query_all(Statement::from_string(DatabaseBackend::Sqlite, METADATA))
        .await?;
    let facts = rows
        .pop()
        .ok_or_else(|| refused("schema classification unavailable"))?;
    let versions: Vec<String> = rows
        .iter()
        .map(|row| row.try_get("", "version"))
        .collect::<Result<_, _>>()?;
    drop(rows);
    snapshot::validate_versions(&versions, false)?;

    if versions
        .iter()
        .any(|version| version == super::super::SCHEMA)
    {
        super::super::receipt_schema::validate(db).await?;
    } else if exists(&facts, "receipt_objects")? {
        return Err(super::super::receipt_schema::invalid());
    }
    snapshot::management_catalog::validate_sqlite_metadata(
        db,
        &versions,
        exists(&facts, "management_shadow")?,
        exists(&facts, "management_objects")?,
    )
    .await?;

    if exists(&facts, "route_shadow")? || facts.try_get::<i32>("", "route_main")? != 1 {
        return Err(refused(
            "unknown, shadowed or incomplete provider route catalog",
        ));
    }
    let route = native::current_catalog()
        .iter()
        .find(|table| table.table == "pin_provider_routes")
        .expect("registered provider route catalog");
    catalog::sqlite_columns(db, route)
        .await
        .map_err(|_| refused("unknown, shadowed or incomplete provider route catalog"))?;

    if receipt {
        if exists(&facts, "receipt_shadow")? {
            return Err(refused("shadowed receipt catalog"));
        }
        if versions.last().map(String::as_str)
            != Some(crate::store::pinning::management::schema::VERSION)
            && exists(&facts, "receipt_partial")?
        {
            return Err(refused("partial management catalog"));
        }
    }
    Ok(versions)
}
