//! Management is an immutable raw closure here, not an executable JSON capture.
use crate::{error::AppResult, store::pinning::management::schema};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

pub(super) async fn validate<C: ConnectionTrait>(db: &C, present: bool) -> AppResult<()> {
    let backend = db.get_database_backend();
    let shadow = match backend {
        DatabaseBackend::Sqlite => {
            "SELECT 1 FROM sqlite_temp_schema WHERE name IN ('pin_management_records','pin_management_retirements','seaql_migrations') LIMIT 1"
        }
        DatabaseBackend::Postgres => {
            "SELECT 1 FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname IN ('pin_management_records','pin_management_retirements','seaql_migrations') LIMIT 1"
        }
        _ => return Err(invalid()),
    };
    if db
        .query_one(Statement::from_string(backend, shadow))
        .await?
        .is_some()
    {
        return Err(invalid());
    }
    if present {
        // Exact registered management columns, PK/FK/checks and native catalog.
        // Never call its Stage7-only full native capture for historical prefixes.
        schema::validate(db).await.map_err(|_| invalid())?;
    } else {
        let sql = if backend == DatabaseBackend::Sqlite {
            "SELECT 1 FROM sqlite_schema WHERE name IN ('pin_management_records','pin_management_retirements') OR tbl_name IN ('pin_management_records','pin_management_retirements') LIMIT 1"
        } else {
            "SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname IN ('pin_management_records','pin_management_retirements') LIMIT 1"
        };
        if db
            .query_one(Statement::from_string(backend, sql))
            .await?
            .is_some()
        {
            return Err(invalid());
        }
    }
    Ok(())
}

fn invalid() -> crate::error::AppError {
    super::super::refused("unknown, shadowed or incomplete management catalog")
}
