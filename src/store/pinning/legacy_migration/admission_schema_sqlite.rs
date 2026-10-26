//! One classification read for versions and bounded catalog-presence facts.
use super::{catalog, native, refused, snapshot};
use crate::error::AppResult;
use sea_orm::{ConnectionTrait, DatabaseBackend, QueryResult, Statement};

#[cfg(test)]
#[path = "admission_schema_sqlite_tests.rs"]
mod tests;

// The version branch retains its original 65-row bound and String decoding.
// The other branch returns only fixed-size EXISTS results, never DDL or cells.
const METADATA: &str = "SELECT 0 AS kind,version,NULL AS facts \
    FROM (SELECT version FROM seaql_migrations ORDER BY version LIMIT 65) \
    UNION ALL SELECT 1,NULL,\
    EXISTS(SELECT 1 FROM sqlite_temp_schema WHERE name IN ('pin_management_records','pin_management_retirements','seaql_migrations')) + \
    2 * EXISTS(SELECT 1 FROM sqlite_schema WHERE name IN ('pin_management_records','pin_management_retirements') OR tbl_name IN ('pin_management_records','pin_management_retirements')) + \
    4 * EXISTS(SELECT 1 FROM sqlite_schema WHERE type IN ('table','view') AND name IN ('pin_legacy_migrations','pin_legacy_migration_items')) + \
    8 * EXISTS(SELECT 1 FROM sqlite_temp_schema WHERE name IN ('seaql_migrations','pin_provider_routes') AND type IN ('table','view')) + \
    16 * EXISTS(SELECT 1 FROM pragma_table_list('pin_provider_routes') WHERE schema='main' AND name='pin_provider_routes' AND type='table') + \
    32 * EXISTS(SELECT 1 FROM sqlite_temp_schema WHERE name IN ('pin_legacy_migrations','pin_legacy_migration_items') OR name GLOB 'pin_management_*' OR tbl_name GLOB 'pin_management_*') + \
    64 * EXISTS(SELECT 1 FROM sqlite_schema WHERE name GLOB 'pin_management_*' OR tbl_name GLOB 'pin_management_*' OR name GLOB 's75_*') \
    ORDER BY kind,version";

struct Facts {
    management_shadow: bool,
    management_objects: bool,
    receipt_objects: bool,
    route_shadow: bool,
    route_main: bool,
    receipt_shadow: bool,
    receipt_partial: bool,
}

struct Metadata {
    versions: Vec<String>,
    facts: Facts,
}

fn decode(rows: Vec<QueryResult>) -> AppResult<Metadata> {
    let mut versions = Vec::new();
    let mut facts = None;
    for row in rows {
        match row.try_get::<i64>("", "kind")? {
            0 => {
                if row.try_get::<Option<i64>>("", "facts")?.is_some() {
                    return Err(refused("schema classification unavailable"));
                }
                versions.push(row.try_get("", "version")?);
            }
            1 => {
                let bits: i64 = row.try_get("", "facts")?;
                if facts.is_some()
                    || row.try_get::<Option<String>>("", "version")?.is_some()
                    || !(0..=127).contains(&bits)
                {
                    return Err(refused("schema classification unavailable"));
                }
                facts = Some(Facts {
                    management_shadow: bits & 1 != 0,
                    management_objects: bits & 2 != 0,
                    receipt_objects: bits & 4 != 0,
                    route_shadow: bits & 8 != 0,
                    route_main: bits & 16 != 0,
                    receipt_shadow: bits & 32 != 0,
                    receipt_partial: bits & 64 != 0,
                });
            }
            _ => return Err(refused("schema classification unavailable")),
        }
    }
    Ok(Metadata {
        versions,
        facts: facts.ok_or_else(|| refused("schema classification unavailable"))?,
    })
}

pub(super) async fn classify<C: ConnectionTrait>(db: &C, receipt: bool) -> AppResult<Vec<String>> {
    let rows = db
        .query_all(Statement::from_string(DatabaseBackend::Sqlite, METADATA))
        .await?;
    let Metadata { versions, facts } = decode(rows)?;
    snapshot::validate_versions(&versions, false)?;

    if versions
        .iter()
        .any(|version| version == super::super::SCHEMA)
    {
        super::super::receipt_schema::validate(db).await?;
    } else if facts.receipt_objects {
        return Err(super::super::receipt_schema::invalid());
    }
    snapshot::management_catalog::validate_sqlite_metadata(
        db,
        &versions,
        facts.management_shadow,
        facts.management_objects,
    )
    .await?;

    if facts.route_shadow || !facts.route_main {
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
        if facts.receipt_shadow {
            return Err(refused("shadowed receipt catalog"));
        }
        if versions.last().map(String::as_str)
            != Some(crate::store::pinning::management::schema::VERSION)
            && facts.receipt_partial
        {
            return Err(refused("partial management catalog"));
        }
    }
    Ok(versions)
}
