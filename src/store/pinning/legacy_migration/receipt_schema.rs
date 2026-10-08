//! Exact known receipt schema, not a migration or repair path.
use crate::error::AppResult;
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
#[path = "receipt_schema_postgres.rs"]
mod postgres;
#[path = "receipt_schema_sqlite.rs"]
mod sqlite;

pub(super) async fn validate<C: ConnectionTrait>(db: &C) -> AppResult<()> {
    match db.get_database_backend() {
        DatabaseBackend::Sqlite => sqlite::validate(db).await,
        DatabaseBackend::Postgres => postgres::validate(db).await,
        _ => Err(invalid()),
    }
}

pub(super) async fn absent<C: ConnectionTrait>(db: &C) -> AppResult<()> {
    let sql = if db.get_database_backend() == DatabaseBackend::Sqlite {
        "SELECT 1 FROM sqlite_schema WHERE type IN ('table','view') AND name IN ('pin_legacy_migrations','pin_legacy_migration_items') LIMIT 1"
    } else {
        "SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname IN ('pin_legacy_migrations','pin_legacy_migration_items') LIMIT 1"
    };
    if db
        .query_one(Statement::from_string(db.get_database_backend(), sql))
        .await?
        .is_some()
    {
        return Err(invalid());
    }
    Ok(())
}

fn invalid() -> crate::error::AppError {
    super::refused(
        "unknown or damaged R26 receipt schema; formal schema integrity must be restored outside CLI",
    )
}
