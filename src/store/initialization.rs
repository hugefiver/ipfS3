//! Startup creates an empty database or opens the current layout without writes.
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, DbErr, Statement, TransactionTrait,
};

use super::migrations;
#[cfg(any(test, feature = "historical-fixtures"))]
pub(crate) mod import_requests;
pub(crate) mod postgres;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaState {
    Empty,
    Current,
    UpgradeRequired,
}

fn required() -> DbErr {
    DbErr::Migration(
        "database schema is not current; supported deployments require --upgrade-database".into(),
    )
}

pub async fn classify<C: ConnectionTrait>(db: &C) -> Result<SchemaState, DbErr> {
    let backend = db.get_database_backend();
    let names = match backend {
        DatabaseBackend::Sqlite => {
            "SELECT name FROM main.sqlite_schema WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%'"
        }
        DatabaseBackend::Postgres => {
            "SELECT c.relname AS name FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relkind IN ('r','p','v','m','f')"
        }
        _ => return Err(DbErr::Migration("unsupported database".into())),
    };
    let tables = db.query_all(Statement::from_string(backend, names)).await?;
    if tables.is_empty() {
        return Ok(SchemaState::Empty);
    }
    for row in tables {
        if row.try_get::<String>("", "name")? == "seaql_migrations" {
            let version = db
                .query_one(Statement::from_string(
                    backend,
                    "SELECT version FROM seaql_migrations ORDER BY version DESC LIMIT 1",
                ))
                .await?;
            if let Some(row) = version
                && row.try_get::<String>("", "version")? == migrations::VERSION
            {
                return Ok(SchemaState::Current);
            }
            break;
        }
    }
    Ok(SchemaState::UpgradeRequired)
}

pub async fn open(db: &DatabaseConnection) -> Result<(), DbErr> {
    match classify(db).await? {
        SchemaState::Current => Ok(()),
        SchemaState::UpgradeRequired => Err(required()),
        SchemaState::Empty if db.get_database_backend() == DatabaseBackend::Postgres => {
            postgres::open(db).await
        }
        SchemaState::Empty => {
            let tx = db.begin().await?;
            // The first statement takes the SQLite writer lock before we read any snapshot.
            tx.execute_unprepared(migrations::CREATE_LEDGER).await?;
            let installed = tx
                .query_one(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    "SELECT version FROM seaql_migrations LIMIT 1",
                ))
                .await?;
            if installed.is_none() {
                let other = tx.query_one(Statement::from_string(DatabaseBackend::Sqlite, "SELECT 1 FROM main.sqlite_schema WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' AND name <> 'seaql_migrations' LIMIT 1")).await?;
                if other.is_some() {
                    return Err(required());
                }
                migrations::install(&tx).await?;
            } else if classify(&tx).await? != SchemaState::Current {
                return Err(required());
            }
            tx.commit().await
        }
    }
}

/// Run with the old gateway stopped and its remote pins released by the operator.
/// Startup never calls this destructive migration.
pub async fn upgrade_existing(db: &DatabaseConnection) -> Result<(), DbErr> {
    let tx = db.begin().await?;
    match db.get_database_backend() {
        DatabaseBackend::Postgres => {
            tx.execute_unprepared("SELECT set_config('search_path', quote_ident(current_schema()) || ', pg_temp', true)").await?;
            tx.execute_unprepared("SELECT pg_advisory_xact_lock(1229997651,1395879239)")
                .await?;
            tx.execute_unprepared("LOCK TABLE seaql_migrations IN EXCLUSIVE MODE")
                .await?;
        }
        DatabaseBackend::Sqlite => {
            tx.execute_unprepared("UPDATE main.seaql_migrations SET version=version WHERE 0")
                .await?;
        }
        _ => return Err(DbErr::Migration("unsupported database".into())),
    }
    migrations::upgrade(&tx).await?;
    tx.commit().await
}
