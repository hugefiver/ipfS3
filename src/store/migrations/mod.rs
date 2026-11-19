use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, Statement};

mod baseline;
mod history;
mod remove_legacy;

#[cfg(any(test, feature = "historical-fixtures"))]
pub(crate) mod bridge;
#[cfg(any(test, feature = "historical-fixtures"))]
#[path = "../../../tests/support/historical_migrations/mod.rs"]
mod fixtures;
#[cfg(any(test, feature = "historical-fixtures"))]
pub use fixtures::*;

pub const VERSION: &str = "m20261120_000001_file_pins";
pub(super) const CREATE_LEDGER: &str = "CREATE TABLE IF NOT EXISTS seaql_migrations (version VARCHAR NOT NULL PRIMARY KEY, applied_at BIGINT NOT NULL)";

async fn finish<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    let revision = match db.get_database_backend() {
        DatabaseBackend::Sqlite => "json_extract(pin_decision,'$.control_revision')",
        DatabaseBackend::Postgres => "(pin_decision::jsonb->>'control_revision')",
        _ => return Err(DbErr::Migration("unsupported database".into())),
    };
    db.execute_unprepared(&format!("CREATE UNIQUE INDEX object_versions_pin_revision ON object_versions({revision}) WHERE pin_decision IS NOT NULL")).await?;
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO seaql_migrations(version,applied_at) VALUES ($1,$2)",
        [VERSION.into(), chrono::Utc::now().timestamp().into()],
    ))
    .await?;
    Ok(())
}

pub(super) async fn install<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    baseline::install(db).await?;
    finish(db).await
}

pub(super) async fn upgrade<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    let rows = db
        .query_all(Statement::from_string(
            db.get_database_backend(),
            "SELECT version FROM seaql_migrations ORDER BY version",
        ))
        .await?;
    let versions: Vec<String> = rows
        .iter()
        .map(|r| r.try_get("", "version"))
        .collect::<Result<_, _>>()?;
    if versions.last().map(String::as_str) == Some(VERSION) {
        return Ok(());
    }
    if !history::supported(&versions) {
        return Err(DbErr::Migration(
            "unsupported source schema: upgrade the previous gateway layout to m20261108_000001_current_pinning_config or later before the file pin upgrade".into(),
        ));
    }
    remove_legacy::run(db).await?;
    db.execute_unprepared("ALTER TABLE object_versions ADD COLUMN pin_decision TEXT")
        .await?;
    db.execute_unprepared(baseline::FILE_PINS).await?;
    finish(db).await
}
