use std::time::Duration;

use anyhow::{Result, anyhow};
use ipfs_s3_gateway::store::SQLITE_BUSY_TIMEOUT_MS;
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection};

pub fn unavailable() -> anyhow::Error { anyhow!("management database unavailable (details withheld)") }

pub async fn connect(url: &str, readonly: bool) -> Result<DatabaseConnection> {
    let sqlite = url.starts_with("sqlite:");
    if sqlite && (url.starts_with("sqlite::memory:") || url.contains("mode=memory")) {
        return Err(unavailable());
    }
    if !sqlite && !url.starts_with("postgres://") && !url.starts_with("postgresql://") { return Err(unavailable()); }
    let mut options = ConnectOptions::new(url.to_owned());
    options.max_connections(1).min_connections(1).sqlx_logging(false)
        .connect_timeout(Duration::from_secs(10)).acquire_timeout(Duration::from_secs(10))
        .max_lifetime(None).idle_timeout(None);
    if sqlite {
        options.map_sqlx_sqlite_opts(move |options| options.read_only(readonly).create_if_missing(false)
            .busy_timeout(Duration::from_millis(SQLITE_BUSY_TIMEOUT_MS as u64)));
    }
    let db = tokio::time::timeout(Duration::from_secs(10), Database::connect(options))
        .await.map_err(|_| unavailable())?.map_err(|_| unavailable())?;
    let settings = async {
        match db.get_database_backend() {
            DatabaseBackend::Sqlite if readonly => { db.execute_unprepared("PRAGMA query_only=ON").await?; }
            DatabaseBackend::Postgres => {
                db.execute_unprepared("SET TIME ZONE 'UTC'").await?;
                db.execute_unprepared("SET statement_timeout='20s'").await?;
                if readonly { db.execute_unprepared("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY").await?; }
            }
            _ => {}
        }
        Ok::<_, sea_orm::DbErr>(())
    }.await;
    if settings.is_err() {
        let _ = db.close().await;
        return Err(unavailable());
    }
    // No Store bootstrap, schema upgrade or damaged-catalog repair. The record
    // authorization and native collector require the existing formal schema.
    Ok(db)
}
