//! Admission classifies the registered catalog without reading business images.
use super::{refused, snapshot};
use crate::{
    error::AppResult,
    store::pinning::management::{catalog, native},
};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

#[path = "admission_schema_sqlite.rs"]
mod sqlite;

pub(crate) async fn admission_schema<C: ConnectionTrait>(db: &C) -> AppResult<Vec<String>> {
    if db.get_database_backend() == DatabaseBackend::Sqlite {
        return sqlite::classify(db, false).await;
    }
    let versions = snapshot::schema(db, false).await?;
    let route = native::current_catalog()
        .iter()
        .find(|table| table.table == "pin_provider_routes")
        .expect("registered provider route catalog");
    catalog::validate_table(db, route)
        .await
        .map_err(|_| refused("unknown, shadowed or incomplete provider route catalog"))?;
    Ok(versions)
}

/// Receipt-only historical reads require a complete registered prefix, never a
/// failed current authority load. No management history is made executable.
pub(in crate::store::pinning) async fn receipt_schema<C: ConnectionTrait>(
    db: &C,
) -> AppResult<Vec<String>> {
    if db.get_database_backend() == DatabaseBackend::Sqlite {
        return sqlite::classify(db, true).await;
    }
    let versions = admission_schema(db).await?;
    let backend = db.get_database_backend();
    let shadow = match backend {
        DatabaseBackend::Postgres => {
            "SELECT 1 FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND (relname IN ('pin_legacy_migrations','pin_legacy_migration_items') OR starts_with(relname,'pin_management_')) LIMIT 1"
        }
        _ => return Err(refused("unsupported receipt catalog")),
    };
    if db
        .query_one(Statement::from_string(backend, shadow))
        .await?
        .is_some()
    {
        return Err(refused("shadowed receipt catalog"));
    }
    if !crate::store::migrations::history::has_final_layout(&versions) {
        let partial = "SELECT 1 WHERE EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND (starts_with(c.relname,'pin_management_') OR starts_with(c.relname,'s75_'))) OR EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=current_schema() AND starts_with(p.proname,'s75_')) OR EXISTS (SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND starts_with(t.tgname,'s75_'))";
        if db
            .query_one(Statement::from_string(backend, partial))
            .await?
            .is_some()
        {
            return Err(refused("partial management catalog"));
        }
    }
    Ok(versions)
}
